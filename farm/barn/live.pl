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

my %config = do '/db/farm/barn/live.config';
print_config_variables();

#
# SQL setup and prepare statements 
#
my ($dbh, $dbn, $stn_select_ips, $stn_select_ipstext, $stn_select_domains, 
    $sth_add_ips, $sth_add_ipstext, $sth_add_domains, $stn_select_flow_today,
    $sth_add_flow_today);
   # note:  dbh -> sqlite; dbn -> mysql on ncollector

my $first_time = 1;  # if set to 1, first pass through digest updates records
                     # from ncollector in last month 
sql_init();

my ($S_eat);  # these are important session variables 
                                   # needed if we want to cleanly shut POE sessions down
                                   # in sub shutdown_gracefully

# First POE Session is called "Eat" (it just consumes data from RABINS and
# updates (via state 'digest') databases, etc.)
#

$S_eat = POE::Session->create(
  inline_states => {

    _start => sub {
      my $service_name = 'Eat';
      $_[KERNEL]->alias_set($service_name);
      $_[HEAP]->{num} = 0;
      
      $logger->info("starting a new POE session called $service_name ... ");
      $_[KERNEL]->delay( 'digest', 0 );
      
    },
    digest => sub { digest_data(),
                    $_[KERNEL]->delay( 'digest', $config{update_delay} ) },

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




$poe_kernel->run( );
exit 0;

sub digest_data {
  $logger->info("DIGEST_DATA - entering .. ");

  my $old_max_modifytime = $dbh->selectrow_array("select datetime('now','-1 month')") if ($first_time); 

# first, process ips

  my ($max_modifytime) = $dbh->selectrow_array("select max(modifytime) from main.ips");
      $max_modifytime  = $old_max_modifytime if ($first_time);

  $logger->debug("DIGEST_DATA - max_modifytime for main.ips = $max_modifytime");

  my $recs = $stn_select_ips->execute($max_modifytime);
  if ($recs) {
    my @a = ();
    while ( @a = $stn_select_ips->fetchrow_array()  ) { 
      $sth_add_ips->execute(@a) ;
    };
    $dbh->commit ;  
    $logger->info("DIGEST_DATA - added $recs records to main.ips");
  };

# second, process ipstext - got to do it as it has the latitude/longitude values

  ($max_modifytime) = $dbh->selectrow_array("select max(modifytime) from main.ipstext");
   $max_modifytime  = $old_max_modifytime if ($first_time);
  $logger->debug("DIGEST_DATA - max_modifytime for main.ipstext = $max_modifytime");

  $recs = $stn_select_ipstext->execute($max_modifytime);
  if ($recs) {
    my @a = ();
    while ( @a = $stn_select_ipstext->fetchrow_array()  ) { 
      $sth_add_ipstext->execute(@a) ;
    };
    $dbh->commit ;  
    $logger->info("DIGEST_DATA - added $recs records to main.ipstext");
  };

# now, process domains

  ($max_modifytime) = $dbh->selectrow_array("select max(modifytime) from main.domains");
   $max_modifytime  = $old_max_modifytime if ($first_time);
  $logger->debug("DIGEST_DATA - max_modifytime for main.domains = $max_modifytime");

  $recs = $stn_select_domains->execute($max_modifytime);
  if ($recs) {
    my @a = ();
    while ( @a = $stn_select_domains->fetchrow_array()  ) { 
      $sth_add_domains->execute(@a) ;
    };
    $dbh->commit ;  
    $logger->info("DIGEST_DATA - added $recs records to main.domains");
  };

  $first_time = 0;  # never do the month-back updates again .. 

  $logger->info("DIGEST_DATA - exiting .. ");

# now, process flow_today

  my ($max_keyid ) = $dbh->selectrow_array("select max(keyid) from main.flow_today");
  $max_keyid = 1 if (! $max_keyid);
  $logger->debug("DIGEST_DATA - max_keyid for main.flow_today = $max_keyid");

  $recs = $stn_select_flow_today->execute($max_keyid);
  if ($recs) {
    my $i = 0;
    my @a = ();
    while ( @a = $stn_select_flow_today->fetchrow_array()  ) { 
      $sth_add_flow_today->execute(@a) ;
      $i++;
      $dbh->commit if (! ($i % 10000));
    };
    $dbh->commit if ($i % 10000) ;  
    $logger->info("DIGEST_DATA - added $recs records to main.flow_today");
  };

  $first_time = 0;  # never do the month-back updates again .. 

  $logger->info("DIGEST_DATA - exiting .. ");
}


sub shutdown_gracefully {

 $logger->info("Shutting everything down - received an interrupt to quit");

 $dbh->disconnect() or warn "Disconnection error: $DBI::errstr\n";
 $logger->info("Sqlite Database Connection shut down ..  ");
 $dbn->disconnect() or warn "Disconnection error: $DBI::errstr\n";
 $logger->info("MySQL Database Connection shut down ..  ");
 
 $poe_kernel->call($S_eat, "shutdown");
 $logger->info("Shut down Session Eat ..  ");
  
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

$dbn = DBI->connect($config{mysql_connection}, $config{mysql_user}, $config{mysql_pwd}, {'RaiseError'=>1})
    or die $DBI::errstr;;

$logger->info("finished opening dbn connection (to pflow on ncollector)");

$dbh = DBI->connect($config{sqlite_connection}, "", "") or die $DBI::errstr;

$dbh->{synchronous} = $config{sqlite_synchronous};
$dbh->{cache_size} =  $config{sqlite_cache_size};
$dbh->{AutoCommit} =  $config{sqlite_autocommit} ;  # note: this must be done after attach database

$stn_select_ips = $dbn->prepare("select ip, ipa, createtime, modifytime, domainid, asnum, ccode 
                        from pflow.ips where modifytime > ?");
                        
$stn_select_flow_today = $dbn->prepare("select keyid, ip_s, ip_d, protocol, port_s, port_d, createtime, starttime, endtime, 
                                  trans, bytes, packets, retrans, jitter_s, jitter_d, tcpflags, dom_s, dom_d,
                                  cc_s, cc_d, as_s, as_d
                        from pflow.flow_today where keyid > ?");
                        
$stn_select_ipstext = $dbn->prepare("select ip, ipname, createtime, modifytime, locationid, regioncode,
                        city, postalcode, latitude, longitude, isp, organization, ccode, ipa, domainid, asnum 
                        from pflow.ipstext where modifytime > ?");
                        
$stn_select_domains = $dbn->prepare("select domainid, organization, shortlabel, isp, city, regioncode, 
                           postalcode, ccode, latitude, longitude, createtime, modifytime 
                        from pflow.domains where modifytime > ?");

$sth_add_ips = $dbh->prepare("replace into main.ips ( ip, ipa, createtime, modifytime, domainid, asnum, ccode ) 
                     values ( ?, ?, ?, ?, ?, ?, ? ) ") or die $DBI::errstr;

$sth_add_flow_today = $dbh->prepare("replace into main.flow_today (keyid, ip_s, ip_d, protocol, port_s, port_d, createtime, starttime, endtime, 
                                  trans, bytes, packets, retrans, jitter_s, jitter_d, tcpflags, dom_s, dom_d,
                                  cc_s, cc_d, as_s, as_d )
                     values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ) ") or die $DBI::errstr;

$sth_add_ipstext = $dbh->prepare("replace into main.ipstext ( ip, ipname, createtime, modifytime, locationid, regioncode,
                        city, postalcode, latitude, longitude, isp, organization, ccode, ipa, domainid, asnum ) 
                     values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ) ") or die $DBI::errstr;

$sth_add_domains = $dbh->prepare("replace into main.domains ( domainid, organization, shortlabel, isp, city, regioncode, 
                           postalcode, ccode, latitude, longitude, createtime, modifytime ) 
                     values (?,?,?,?,?,?,?,?,?,?,?,?) ") or die $DBI::errstr;

};


