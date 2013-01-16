#!/usr/bin/perl
use warnings;
use strict;

use POE;
use POE::Filter::Reference;
use XML::Simple; # qw(:strict);
use POE::Component::IKC::Server;
use POE::Component::IKC::Responder;

use DBI;
use DBD::mysql;

use Log::Log4perl;
Log::Log4perl->init_and_watch("logfile.conf", 'HUP');

my $logger = Log::Log4perl->get_logger();

## begin CONFIG

my $connstr = 'DBI:mysql:pflow:ncollector.gloriad.org:mysql_socket=/private/tmp/mysql.sock';
my $user    = '*****';
my $passwd  = '*********';
my $dbh = DBI->connect($connstr, $user, $passwd, {'RaiseError'=>1})
    or die $DBI::errstr;;

my $SERVER_ALIAS = "GLORIADFlow";
my $PORT_NUMBER = 12546;
my $ENDSTREAM = "<ENDXML>"; 

## end   CONFIG

# set up global countries hash

POE::Component::IKC::Responder->spawn();
$logger->info("finished spawning ikc Responder client");


my %countries;

my $sql = qq {
select code, country from pflow.ccodes
       };
my $sth = $dbh->prepare( $sql );
my $result = $sth->execute();

while (my ($code, $name) = $sth->fetchrow_array())  {
       $countries{$code} = $name;
};

$countries{'EG '} = 'Egypt';

$logger->info("finished loading pflow.ccodes");


# set up world regions hash

my %worldclass;

 $sql = qq {
select worldid, worldclass from pflow.worldclass
       };
 $sth = $dbh->prepare( $sql );
 $result = $sth->execute();

while (my ($code, $name) = $sth->fetchrow_array())  {
       $worldclass{$name} = $code;
};
$logger->info("finished loading pflow.worldclass");

# set up government id hash

my %govid;

$sql = qq {
select govid, mapto from pflow.govagencies
       };
$sth = $dbh->prepare( $sql );
$result = $sth->execute();

while (my ($code, $name) = $sth->fetchrow_array())  {
       $govid{$name} = $code;
};
$logger->info("finished loading pflow.govagencies");

# set up government id hash

my %orgclass;

$sql = qq {
select orgid, organization from pflow.orgclass
       };
$sth = $dbh->prepare( $sql );
$result = $sth->execute();

while (my ($code, $name) = $sth->fetchrow_array())  {
       $orgclass{$name} = $code;
};

$logger->info("finished loading pflow.orgclass");


POE::Component::IKC::Server->spawn(
  port => $PORT_NUMBER,
  name => 'GLORIADFlow',
  verbose => 1,
) ; 

$logger->info("finished spawning IKC::Server");


POE::Session->create(
  inline_states => {

    _start => sub {
      my ($kernel, $heap) = @_[KERNEL, HEAP];
      my $service_name = 'CoreServices';
      $kernel->alias_set($service_name);
      $heap->{num} = 0;
      $kernel->post(IKC => publish => $service_name, ['topu', 'alive']);
      
      $logger->info("starting a new POE session ... ");;
      
    },

    topu => \&TopUsers,

    alive => sub { my ($name) = @{ $_[ARG0] } ; $logger->info("in alive - name = $name"); return [$name, 1]; },

    _stop => sub { $logger->info("Stopping $0"); },

  }
);


$poe_kernel->run( );
exit 0;


   
sub TopUsers {
  my ($heap,  $xml ) = @_[ HEAP, ARG0 ];

my ($dom_s, $dom_d, $source, $dest, $dbytes, $dpackets, $dretransmits,
       $pktloss, $secs, $mintime, $maxtime, $pbytes, $bps,
       $dur, $seconds, $load, $city_s, $ccode_s, $city_d, $ccode_d, $country_s, $country_d, $title) = "";

$logger->debug("in TopUsers .. xml = $xml");

if ($xml eq "") { $logger->debug("xml is blank .. returning .. "); return; };

my $x = XMLin($xml);
my $cc = $x->{parameters}->{country} || "";  
my $region = $x->{parameters}->{region} || ""; 
my $usgov = $x->{parameters}->{usgov} || "";   
my $whereclause = "";

if ($region) { 
   my $regioncode = $worldclass{"$region"};

  $whereclause = qq {
    where world_s = '$regioncode' or world_d = '$regioncode' 
    };
    
  if ($region eq "Scandinavia") { $title = "Nordic Countries"; } 
  else {$title = $region; }; 
    
} elsif ($usgov eq 'NSF') {
  my $ucode = $orgclass{'University'};
  $whereclause = qq {
   where (ccode_s = 'US' and orgclass_s = '$ucode') or (ccode_d = 'US' and orgclass_d = '$ucode')
   };
   
   $title = "US NSF (and all Universities)";
   
} elsif ($usgov) {
  my $gid = $govid{'$usgov'};
  $whereclause = qq {
   where (ccode_s = 'US' and govid_s = '$gid') or (ccode_d = 'US' and govid_d = '$gid')
   };
   
   $title = "US $usgov";
   
} elsif ($cc) {
  $whereclause = qq {   
   where ccode_s = '$cc' or ccode_d = '$cc'  
  };
  
  $title = $countries{$cc} || $cc ;
  
} else {

  $title = "All GLORIAD Traffic";
  
  };

my $sql = qq {
select dom_s, dom_d, 
       label_s, city_s, ccode_s, 
       label_d, city_d, ccode_d, 
       sum(bytes_d) as dbytes,
       sum(packets_d) as dpackets,
       sum(retrans_d) as dretransmits,
       (sum(retrans_d) / sum(packets_d)) * 100 as pktloss, 
       endtime - starttime as secs, 
       min(starttime), max(endtime), max(endtime) - min(starttime) as secs,
       avg(load_d), avg(dur)
from pflow.liveflow 
$whereclause
group by dom_s, dom_d order by dbytes desc limit 50
       };

$logger->debug("sql = $sql");

my $sth = $dbh->prepare( $sql );
my $result = $sth->execute();

my $sendback = "";
my $timerange = "";
my $minstarttime = my $maxendtime = "";

while (($dom_s, $dom_d, $source, $city_s, $ccode_s, 
        $dest, $city_d, $ccode_d, $dbytes, $dpackets, $dretransmits,
       $pktloss, $secs, $mintime, $maxtime, $seconds, $load, $dur) = $sth->fetchrow_array())  {
       
       if ($dur == 0) { $bps = "-"; } 
       else { 
         # $bps = $dbytes * 8 / $dur; 
         $bps = $dbytes * 8 / $seconds;
         if ($bps > 1000000) { $bps = sprintf("%.1f", $bps / 1000000) . " Mbps"; }
         elsif ($bps > 1000) { $bps = sprintf("%.1f", $bps / 1000) . " Kbps"; }
         else { $bps = sprintf("%.1f", $bps) . " bytes";  };
       };
       
       if ($dbytes > 1000000000) { $pbytes = sprintf("%.1f", ($dbytes / 1000000000)) . " GB   "; }
       elsif ($dbytes > 1000000) { $pbytes = sprintf("%.1f", ($dbytes / 1000000)) . " MB   "; }
       elsif ($dbytes > 1000) { $pbytes = sprintf("%.1f", ($dbytes / 1000)) . " KB   "; }
       else { $pbytes = sprintf("%f", $dbytes) . " bytes"; };
       
       $pktloss = sprintf("%.1f", $pktloss) . " %";
       
       if ($dom_s) {
          if ($city_s) { $source .= " ($city_s, $countries{$ccode_s})"; } 
          else { $source .= " ($countries{$ccode_s})"; } 
       } 
       if ($dom_d) {
          if ($city_d) { $dest .= " ($city_d, $countries{$ccode_d})"; } 
          else { $dest .= " ($countries{$ccode_d})"; } 
       };
       
       my $line = join("\t", $source, $dest, $pbytes, $bps, $dpackets, $pktloss);
       $minstarttime = $mintime if ($minstarttime lt $mintime); 
       $maxendtime = $maxtime if ($maxtime gt $maxendtime);
       $sendback .= $line . "\n";
};

if ($minstarttime) {
  $timerange = "Time Period (US East Coast): $minstarttime - $maxendtime";
} else {
  $timerange = "No Data Reported";
  };
  
#my $headers = join("\t", "Source Domain", "Dest Domain", 
#                   "Source Institution", "Dest Institution",
#                   "Bytes", "Packets", "Retransmits", "Packet Loss %");

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

# print "-------------------\n$outp\n----------------------\n";

return $outp;

# $kernel->yield( 'ClientWrite', "$outp");
# $kernel->delay_set( 'UpdateUsers', 5, "");

};
