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
use Log::Log4perl::DateFormat;
Log::Log4perl->init("diary.conf");
my $format = Log::Log4perl::DateFormat->new("ABSOLUTE");

my $logger = Log::Log4perl->get_logger();

my %config = do 'live.config';
print_config_variables();

#
# SQL setup and prepare statements 
#
my ($dbh, $sth_add, $sth_update_mem_ips, $sth_update_mem_domains, 
    $sth_lookup_ip, $sth_lookup_domain, $sth_add_rabins);

my $max_modifytime_ips;   # important this keeps track of max(modifytime) in feed.ips
                          # table - important so that we can keep mem.ips updated
my $max_modifytime_domains;   # and this one for domains ..  

sql_init();


#
# open the RABINS command (defined in %config) to begin data flowing 
#
$logger->info("opening rabins command ... ");
open (RABINS, $config{rabins_command});
$logger->info("finished opening rabins command ... ");


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
      $_[KERNEL]->delay( 'groom', 10 );
      
    },
    groom => sub { 
       $logger->info("In Session Clean .. state groom ");

       my $new_max_modifytime_ips = $dbh->selectrow_array("select max(modifytime) from feed.ips");
       my $new_max_modifytime_domains = $dbh->selectrow_array("select max(modifytime) from feed.domains");

       my $res = $sth_update_mem_ips->execute($max_modifytime_ips); 
       $logger->info("In Session Clean .. just updated $res ips records ");
          $res = $sth_update_mem_domains->execute($max_modifytime_domains); 
       $logger->info("In Session Clean .. just updated $res domains records ");
    
       $max_modifytime_ips = $new_max_modifytime_ips;
       $max_modifytime_domains = $new_max_modifytime_domains;
    
       $_[KERNEL]->delay( 'groom', $config{cleaning_delay} );
       
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

$logger->info("finished spawning IKC::Server port/name = ", $config{port_number}, " / ", $config{server_alias});


$S_answer = POE::Session->create(
  inline_states => {

    _start => sub {
      my ($kernel, $heap) = @_[KERNEL, HEAP];
      my $service_name = 'Answer';
      $kernel->alias_set($service_name);
      $heap->{num} = 0;
      $kernel->post(IKC => publish => $service_name, ['glotop', 'gloearth', 'alive']);
      
      $logger->info("starting a new POE session called $service_name... ");;
      
    },

    glotop => \&GloTOP,
    gloearth => \&GloEARTH,
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



sub GloTOP {
  my ($heap,  $xml ) = @_[ HEAP, ARG0 ];

my ($dom_s, $dom_d, $source, $dest, $dbytes, $dpackets, $dretransmits,
       $pktloss, $secs, $mintime, $maxtime, $pbytes, $bps,
       $dur, $seconds, $load, $city_s, $ccode_s, $city_d, $ccode_d, $country_s, $country_d, $title) = "";

  $logger->info("entering GloTOP subroutine");

if ($xml eq "") { $logger->debug("xml is blank .. returning .. "); return; };

my $x = XMLin($xml);
my $cc =     $x->{parameters}->{country} || "";  
my $region = $x->{parameters}->{region}  || ""; 
my $govid =  $x->{parameters}->{usgov}   || "";   
my $whereclause = "";

if ($region) { 
   my ($regioncode) = $dbh->selectrow_array("select worldid from mem.worldclass where wclass = '$region'");

  $whereclause = qq {
    where world_s = '$regioncode' or world_d = '$regioncode' 
    };
    
  if ($region eq "Scandinavia") { $title = "Nordic Countries"; } 
  else {$title = $region; }; 
    
} elsif ($govid eq 'NSF') {
  $whereclause = qq {
   where (cc_s = 'US' and org_s = '5') or (cc_d = 'US' and org_d = '5')
   };
   
   $title = "US NSF (and all Universities)";
   
} elsif ($govid) {
  my ($gid,$gname) = $dbh->selectrow_array("select govid, agency from mem.govagencies where mapto = '$govid'");
  $whereclause = qq {
   where (gov_s = '$gid') or (gov_d = '$gid')
   };
   
   $title = "Government Agency: $gname";
   
} elsif ($cc) {
  $whereclause = qq {   
   where cc_s = '$cc' or cc_d = '$cc'  
  };
  my ($country) = $dbh->selectrow_array("select country from mem.ccodes where code = '$cc'");
  
  $title = $country || $cc ;
  
} else {

  $title = "All GLORIAD Traffic";
  
  };

my $sql = qq {
select dom_s, dom_d, 
       name_s, city_s, cc_s, 
       name_d, city_d, cc_d, 
       sum(bytes) as dbytes,
       sum(packets) as dpackets,
       sum(retrans) as dretransmits,
       (sum(retrans) / sum(packets)) * 100 as pktloss, 
       strftime("%f6",(max(endtime) - min(starttime) ) ) as secs, 
       strftime("%Y-%m-%d %H:%M:%f6",min(starttime),'localtime') as minstart, strftime("%Y-%m-%d %H:%M:%f6",max(endtime),'localtime') as maxend,
       sum(bytes) * 8 / strftime("%f",(max(endtime) - min(starttime) )  ) as avgload,
       country_s, country_d
from mem.rabins 
$whereclause
group by dom_s, dom_d order by dbytes desc limit 50
       };

$logger->debug("sql = $sql");

my $sth = $dbh->prepare( $sql );
my $result = $sth->execute();

my $sendback = "";
my $timerange = "";
my $minstarttime = my $maxendtime = "";

while (my ($dom_s, $dom_d, $source, $city_s, $ccode_s, 
        $dest, $city_d, $ccode_d, $dbytes, $dpackets, $dretransmits,
       $pktloss, $secs, $mintime, $maxtime, $load, $country_s, $country_d) = $sth->fetchrow_array())  {
       
       my $bps = $load;
         if ($bps > 1000000) { $bps = sprintf("%.1f", $bps / 1000000) . " Mbps"; }
         elsif ($bps > 1000) { $bps = sprintf("%.1f", $bps / 1000) . " Kbps"; }
         else { $bps = sprintf("%.1f", $bps) . "  bps";  };
       
       if ($dbytes > 1000000000) { $pbytes = sprintf("%.1f", ($dbytes / 1000000000)) . " GB   "; }
       elsif ($dbytes > 1000000) { $pbytes = sprintf("%.1f", ($dbytes / 1000000)) . " MB   "; }
       elsif ($dbytes > 1000) { $pbytes = sprintf("%.1f", ($dbytes / 1000)) . " KB   "; }
       else { $pbytes = sprintf("%f", $dbytes) . " bytes"; };
       
       $pktloss = sprintf("%.3f", $pktloss) . " %";
       
       if ($dom_s) {
          if ($city_s) { $source .= " ($city_s, $country_s)"; } 
          else { $source .= " ($country_s)"; } 
       } 
       if ($dom_d) {
          if ($city_d) { $dest .= " ($city_d, $country_d)"; } 
          else { $dest .= " ($country_d)"; } 
       };
       
       my $line = join("\t", $source, $dest, $pbytes, $bps, $dpackets, $pktloss);
       $minstarttime = $mintime if ($minstarttime lt $mintime); 
       $maxendtime = $maxtime if ($maxtime gt $maxendtime);
       $sendback .= $line . "\n";
};

$minstarttime =~ s/(.*)\.\d+$/$1/;
$maxendtime   =~ s/(.*)\.\d+$/$1/;

if ($minstarttime) {
  $timerange = "Time Period (US East Coast): $minstarttime - $maxendtime";
} else {
  $timerange = "No Data Reported";
  };
  
my $outp = qq { 
<dispatch>
   <msg>UpdateControl</msg>
   <parameters></parameters>
   <card>TopUsersCard</card>
   <control>TopUsers</control>
   <timerange>$timerange</timerange>
   <title>$title</title>
   <data>$sendback</data>
</dispatch>
};

}

sub GloEARTH {
  $logger->info("entering GloEARTH subroutine");






}

sub digest_rabins {
  $logger->info("entering digest_rabins");
  
  no warnings 'uninitialized'; # otherwise, will drive you crazy .. 

  my @a = split("\n", $_[ARG0]);
  
  $dbh->do("delete from mem.rabins");

    my ($name_s, $cc_s, $city_s, $lat_s, $long_s, $org_s, $world_s, $gov_s, $disc_s,
      $org_s_l, $world_s_l, $gov_s_l, $disc_s_l, 
      $name_d, $cc_d, $city_d, $lat_d, $long_d, $org_d, $world_d, $gov_d, $disc_d,
      $org_d_l, $world_d_l, $gov_d_l, $disc_d_l, $country_s, $country_d);
 
  my $numrecs = 0; 

  foreach (@a) {
    next if (/^man/);   # get rid of START management record (at begin of rabins burst)
    next if (/^Proto/);  # get rid of column headers line (can't suppress from rarc file)
    my ($proto, $state, $srcid, $stime, $ltime, $dur, $saddr, $dir, $daddr, 
        $sport, $dport, $spkts, $sbytes, $sload, $psloss, $sloss,  
        $smac, $dmac, $svid, $dvid, $sjit, $djit) = split('\|');
    
      next if ($saddr > 4294967296);  # sadly throw away if IPv6 address .. 
      next if ($daddr > 4294967296);  # sadly throw away if IPv6 address .. 
      next if ($sbytes < 100000 );   # not sure why we see records with 0 bytes but we do .. 
    
    # hopefully, the following lookups are extremely fast as they're accessing 
    # indexed memory table (mem.ips) 
    
    $sth_lookup_ip->execute($saddr);    
    my ($dom_s, $as_s) = $sth_lookup_ip->fetchrow_array();    
    $dom_s = 0 if (! $dom_s); $as_s = 0 if (! $as_s);

    $sth_lookup_ip->execute($daddr);    
    my ($dom_d, $as_d) = $sth_lookup_ip->fetchrow_array();    
     $dom_d = 0 if (! $dom_d); $as_d = 0 if (! $as_d);

    next if (! $dom_s);
    next if (! $dom_d);
    
    $sth_lookup_domain->execute($dom_s);
      ($name_s, $cc_s, $city_s, $lat_s, $long_s, $org_s, $world_s, $gov_s, $disc_s,
      $org_s_l, $world_s_l, $gov_s_l, $disc_s_l, $country_s) =
         $sth_lookup_domain->fetchrow_array();
        
    $sth_lookup_domain->execute($dom_d);
      ($name_d, $cc_d, $city_d, $lat_d, $long_d, $org_d, $world_d, $gov_d, $disc_d,
      $org_d_l, $world_d_l, $gov_d_l, $disc_d_l, $country_d) =
         $sth_lookup_domain->fetchrow_array();
        
#  $logger->info("getting ready to do sth_add_rabins");


    $sth_add_rabins->execute($srcid, $saddr, $daddr, $proto, $sport, $dport, $stime, $ltime, $dur, 
                      $sbytes, $spkts, $sloss, $sload, $dom_s, $dom_d, $as_s, $as_d, 
                      $name_s, $name_d, $cc_s, $cc_d, $city_s, $city_d, 
                      $lat_s, $lat_d, $long_s, $long_d, 
                      $org_s, $org_d, $world_s, $world_d,
                      $gov_s, $gov_d, $disc_s, $disc_d, 
                      $org_s_l, $org_d_l, $world_s_l, $world_d_l,
                      $gov_s_l, $gov_d_l, $disc_s_l, $disc_d_l, $country_s, $country_d );
     $numrecs++;
  # $logger->info("finished finished doing sth_add_rabins - numrecs = $numrecs ");
  };
  $dbh->commit;  
  
  $logger->info("exiting digest_rabins - processed $numrecs records ");

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
$dbh->do("attach database '/db/farm/barn/feed.db' as feed");  # setup access to main feed database 

$dbh->do("vacuum");  # might as well do this on initialization (next statements take a long time)

$dbh->{AutoCommit} =  $config{sqlite_autocommit} ;  # note: this must be done after attach database


$logger->info("SQL_INIT - Entering ... (this is going to take awhile - lots of ips to load ..)  ");

                        
# next, load up database records into the mem (:memory:) database.  First, grab max-modifytime
# from feed.ips (so we'll know which records to grab later to update the mem.ips table)

$max_modifytime_ips = $dbh->selectrow_array("select max(modifytime) from feed.ips");
$max_modifytime_domains = $dbh->selectrow_array("select max(modifytime) from feed.domains");

$dbh->do("create table mem.ips as select ip, domainid, asnum from feed.ips  ");  # ip database (huge)
$dbh->do("create unique index mem.ip on ips (ip)");  # create index

$dbh->do("create table mem.domains as select * from feed.domains");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.domainid on domains (domainid)");  # create index

$dbh->do("create table mem.worldclass as select * from feed.worldclass");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.worldid on worldclass (worldid)");  # create index

$dbh->do("create table mem.orgclass as select * from feed.orgclass");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.orgid on orgclass (orgid)");  # create index

$dbh->do("create table mem.disciplines as select * from feed.disciplines");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.discid on disciplines (discid)");  # create index

$dbh->do("create table mem.govagencies as select * from feed.govagencies");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.govid on govagencies (govid)");  # create index

$dbh->do("create table mem.ccodes as select * from feed.ccodes");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.cccode on ccodes (code)");  # create index

$dbh->do("create table mem.colors as select * from feed.colors");  # for fast retrieval of course .. 
$dbh->do("create index mem.colorcode on colors (scheme, code)");  # create index

$dbh->do("create table mem.asnums as select asnum, asname, ccode, modifytime from feed.asnums");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.asnum on asnums (asnum)");  # create index

$dbh->do("create table mem.protocols as select * from feed.protocols");  # for fast retrieval of course .. 
$dbh->do("create unique index mem.protocolid on protocols (protocolid)");  # create index

$dbh->do("create table mem.rabins as select * from main.rabins");  # might as well keep in memory .. 
$dbh->do("create index mem.doms on rabins (dom_s, dom_d)");   

# next statement is periodically executed in clean session to keep the memory 
# argus table updated.  Key attribute to be passed to this prepared statement is
# a global vairable:  $max_modifytime_ips. 
             
$sth_update_mem_ips = $dbh->prepare(
                qq { replace into mem.ips select ip, domainid, asnum from feed.ips
                     where modifytime >= ? 
                        } );

$sth_update_mem_domains = $dbh->prepare(
                qq { replace into mem.domains select * from feed.domains
                     where modifytime >= ?  
                        } );

$sth_lookup_ip = $dbh->prepare("select domainid, asnum from mem.ips where ip = ?");


$sth_lookup_domain = $dbh->prepare("select shortlabel, d.ccode, city, latitude,
       longitude, orgclass, d.worldclass, d.govid, d.discipline, orgclass.organization, wclass, 
       agency, disciplines.discipline, c1.country
      from domains as d left join orgclass on (orgclass = orgid) 
        left join worldclass on (d.worldclass = worldid) 
        left join govagencies on (d.govid = govagencies.govid) 
        left join disciplines on (d.discipline = discid) 
        left join ccodes as c1 on (d.ccode = c1.code) where domainid = ?");

$sth_add_rabins = $dbh->prepare(qq { 
 insert into mem.rabins (srcid, ip_s, ip_d, protocol, 
 port_s, port_d, starttime, endtime, dur,  bytes, packets, retrans, loadt, 
 dom_s, dom_d, as_s, as_d, name_s, name_d, cc_s, cc_d, city_s, city_d, 
lat_s, lat_d, long_s, long_d, 
org_s, org_d, world_s, world_d,
gov_s, gov_d, disc_s, disc_d, 
org_s_l, org_d_l, world_s_l, world_d_l,
gov_s_l, gov_d_l, disc_s_l, disc_d_l, country_s, country_d  ) values (
 ?, ?, ?, ?, ?, ?, julianday(?,'unixepoch'), julianday(?,'unixepoch'), 
 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?  )
    }  );


$logger->info("SQL_INIT - Exiting (loaded mem.ips, created index, etc.) ... ");


};


