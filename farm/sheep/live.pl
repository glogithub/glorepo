#!/usr/bin/perl
use warnings;
use strict;

use POE;
use POE::Filter::Reference;
use XML::Simple; # qw(:strict);
use POE::Component::IKC::Server;
use POE::Component::IKC::Responder;
use POE::Wheel::ReadWrite;
use Data::Dumper;
use DBI;
use DBD::SQLite;
use Log::Log4perl;
Log::Log4perl->init("diary.conf");
my $logger = Log::Log4perl->get_logger();

my %config = do 'live.config';
print_config_variables();

#
# SQL setup and prepare statements 
#
my ($dbh, $sth_add, $sth_delete, $sth_delete_memory);

sql_init();


#
# open the RABINS command (defined in %config) to begin data flowing 
#
open (RABINS, $config{rabins_command});


my ($S_eat, $S_clean, $S_answer);  # these are important session variables 
                                   # needed if we want to cleanly shut POE sessions down
                                   # in sub shutdown_gracefully

# First POE Session is called "Eat" (it just consumes data from RABINS and
# updates (via state 'digest') databases, etc.)
#

$S_eat = POE::Session->create(
  inline_states => {

    _start => sub {
        $_[HEAP]->{reader} = POE::Wheel::ReadWrite->new(
         Handle  => \*RABINS,   
         InputFilter => POE::Filter::Line->new(InputRegexp => 'man\|STP.*\n'),
                                               InputEvent => 'digest',),
      my $service_name = 'Eat';
      $_[KERNEL]->alias_set($service_name);
      $_[HEAP]->{num} = 0;
      
      $logger->info("starting a new POE session called $service_name ... ");
      $_[KERNEL]->delay( 'eat', 0 );
      
    },
    digest => \&digest_rabins, 

    _stop => \&shutdown_gracefully ,
    
    shutdown => sub  {
        my ($kernel, $session, $heap) = @_[KERNEL, SESSION, HEAP];

        $logger->info("Shutdown Service Eat");

        # delete all wheels.
        delete $heap->{reader};

        # clear your alias
        $kernel->alias_remove($heap->{Eat});

        # clear all alarms you might have set
        $kernel->alarm_remove_all();

        # get rid of external ref count
        $kernel->refcount_decrement($session, 'num');

        # propagate the message to children
        $kernel->post($heap->{child_session}, 'shutdown');
        return;
    }
  }
);


# Second POE Session is called "Clean" (it periodically trims tables, resets
# log files, etc. (basically, housekeeping tasks))
#

$S_clean = POE::Session->create(
  inline_states => {

    _start => sub {
      my $service_name = 'Clean';
      $_[KERNEL]->alias_set($service_name);
      $_[HEAP]->{num} = 0;
      
      $logger->info("starting a new POE session called $service_name ... ");
      $_[KERNEL]->delay( 'trim', 0 );
      
    },
    trim => sub { 
       $logger->info("In Session Clean .. state trim ");
       
       my $res = $sth_delete->execute();
       $_[KERNEL]->delay( 'trim', $config{cleaning_delay} );
       
    }, 

    _stop => \&shutdown_gracefully ,
     
    shutdown => sub  {
        my ($kernel, $session, $heap) = @_[KERNEL, SESSION, HEAP];

        $logger->info("Shutdown Service Clean");

        # clear your alias
        $kernel->alias_remove($heap->{Clean});

        # clear all alarms you might have set
        $kernel->alarm_remove_all();

        # get rid of external ref count
        $kernel->refcount_decrement($session, 'num');

        # propagate the message to children
        $kernel->post($heap->{child_session}, 'shutdown');
        return;
    }
  }
);



# POE::Component::IKC::Responder->spawn();
# $logger->info("finished spawning ikc Responder client");
# 
# 
POE::Component::IKC::Server->spawn(
  port => $config{port_number},
  name => $config{server_alias},
  verbose => 1,
) ; 

$logger->info("finished spawning IKC::Server");


$S_answer = POE::Session->create(
  inline_states => {

    _start => sub {
      my ($kernel, $heap) = @_[KERNEL, HEAP];
      my $service_name = 'Answer';
      $kernel->alias_set($service_name);
      $heap->{num} = 0;
      $kernel->post(IKC => publish => $service_name, ['report', 'alive']);
      
      $logger->info("starting a new POE session called $service_name... ");;
      
    },

#    report => \&TopUsers,
    alive => sub { my ($name) = @{ $_[ARG0] } ; $logger->info("in alive - name = $name"); return [$name, 1]; },

    _stop => \&shutdown_gracefully ,
     
    shutdown => sub  {
        my ($kernel, $session, $heap) = @_[KERNEL, SESSION, HEAP];

        $logger->info("Shutdown Service Answer");

        # delete all wheels.
        delete $heap->{reader};

        # clear your alias
        $kernel->alias_remove($heap->{Answer});

        # clear all alarms you might have set
        $kernel->alarm_remove_all();

        # get rid of external ref count
        $kernel->refcount_decrement($session, 'num');

        # propagate the message to children
        $kernel->post($heap->{child_session}, 'shutdown');
        return;
    }


  }
);



$poe_kernel->run( );
exit 0;



sub digest_rabins {
  $logger->info("Sheep: digest_rabins");
  
  my @a = split("\n", $_[ARG0]);
  
  foreach (@a) {
    next if (/^man/);   # get rid of START management record (at begin of rabins burst)
    next if (/^Proto/);  # get rid of column headers line (can't suppress from rarc file)
    my ($proto, $state, $srcid, $stime, $load, $bytes, $pkts, $ploss) = split('\|');
    $sth_add->execute($srcid, $stime, $load, $bytes, $pkts, $ploss);
    $logger->debug("record: $proto, $state, $srcid, $stime, $load, $bytes, $pkts, $ploss");
  };
  $dbh->commit;  

};  

sub shutdown_gracefully {

 $logger->info("Shutting everything down - received an interrupt to quit");

 $dbh->disconnect() or warn "Disconnection error: $DBI::errstr\n";
 
 $logger->info("Sqlite Database Connection shut down ..  ");
 
 $poe_kernel->call($S_eat, "shutdown");
 $logger->info("Shut down Session Eat ..  ");
 $poe_kernel->call($S_clean, "shutdown");
 $logger->info("Shut down Session Clean ..  ");
 $poe_kernel->call($S_answer, "shutdown");
 $logger->info("Shut down Session Answer ..  ");
 
 $poe_kernel->stop();
 $logger->info("Just called poe_kernel->stop .. all should be dead now ..");
 print "Bye!\n";
 exit(0);
};


sub print_config_variables {

# config is a globally defined hash 

$logger->info( "preparing to print config variables " ) ;

foreach my $key (keys %config) {
   $logger->info( "config variable $key: ", $config{$key}  ) ;
   };
};

sub sql_init {
$dbh = DBI->connect($config{sqlite_connection}, "", "") or die $DBI::errstr;

$dbh->{synchronous} = $config{sqlite_synchronous};
$dbh->{cache_size} =  $config{sqlite_cache_size};

$dbh->do("attach database ':memory:' as mem");      # setup access to new in-memory database 
$dbh->{AutoCommit} =  $config{sqlite_autocommit} ;  # note: this must be done after attach database

$sth_add = $dbh->prepare("insert into main.argus_sources ( srcid, stime, load, bytes, packets, pct_loss) 
                     values ( ?, julianday(?,'unixepoch'), ?, ?, ?, ? ) ") or die $DBI::errstr;

$logger->info("datetime_trim = ", $config{datetime_trim});

$sth_delete = $dbh->prepare(
                qq { delete from main.argus_sources where 
                          stime < (select julianday(max(stime), \'$config{datetime_trim}\') 
                          from main.argus_sources ) 
                        } );
                        
# next, load up database records into the mem (:memory:) database

$dbh->do("create table mem.srcid as select * from main.srcid");  # source IDs (i.e, Chicago, Seattle)
$dbh->do(qq { 
         create table mem.argus_sources as select * from main.argus_sources
          where stime > (select julianday(max(stime), '$config{datetime_memory}') 
                         from main.argus_sources ) 
             } );
             
# next statement is periodically executed in Clean session to keep the memory 
# argus table trimmed.  Key attribute is $config{datetime_memory}. 
             
$sth_delete_memory = $dbh->prepare(
                qq { delete from mem.argus_sources where 
                          stime < (select julianday(max(stime), \'$config{datetime_memory}\') 
                          from mem.argus_sources ) 
                        } );
                        


};


