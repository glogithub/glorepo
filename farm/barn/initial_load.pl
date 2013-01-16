#!/usr/bin/perl
use warnings;
use strict;

use POE;
use POE::Filter::Reference;
use XML::Simple; # qw(:strict);

use DBI;
use DBD::mysql;
use DBD::SQLite;

use Log::Log4perl;
Log::Log4perl->init_and_watch("logfile.conf", 'HUP');

my $logger = Log::Log4perl->get_logger();

# note:  the %dbs hash indicates the tables that are to be loaded.
#        they key (first value) is name of table in sqlite database
#        the value (second in pair) is name of table in mysql database
#        if differently named (otherwise, it's the same as for sqlite)
 
my %dbs = (
  'apps', '', 
  'asnums', '', 
  'ccodes', '', 
  'classes', '', 
  'colors', '', 
  'disciplines', '', 
  'domains', '', 
  'govagencies', '', 
  'ips', '', 
  'ipstext', '', 
  'liveflow', '', 
  'macaddrs', '', 
  'orgclass', '', 
  'protocols', '', 
  'worldclass', '', 
);


my $trans = 250000;  # only commit every 250,000 adds

# open connection to pflow tables on ncollector

my $connstr = 'DBI:mysql:pflow:ncollector.gloriad.org:mysql_socket=/private/tmp/mysql.sock';
my $user    = '*****';
my $passwd  = '********';
my $db1 = DBI->connect($connstr, $user, $passwd, {'RaiseError'=>1})
    or die $DBI::errstr;;

$logger->info("finished opening db1 connection (to pflow on ncollector)");

# open connection to local main.db file (sqlite3)

$connstr = 'DBI:SQLite:dbname=main.db';
my $db2 = DBI->connect($connstr, "", "") or die $DBI::errstr;;
$db2->{AutoCommit} = 0 ;  # always use transaction mode (i.e., commit is required before inserts are made)
$db2->{synchronous} = 0;  
$db2->{cache_size} = 1000000;

$logger->info("finished opening db2 connection (to main.db locally)");

my $res;

foreach my $key (keys %dbs) {

  $res = $db2->do("delete from $key");

  $logger->info("$res records deleted from main.db, table $key");

  my $db = $dbs{$key} || $key;
  my $sth = $db1->prepare("select * from $db ");
  my $recs = $sth->execute || die $DBI::errstr;
  my $names = $sth->{NAME};   # array reference with columns

  my @names = @$names;
  my $numfields = @names;

  my $tmp = join(",", @names);
  my $placeholders = "";
  foreach (@names) { $placeholders .= "?,"; };
  $placeholders =~ s/,$//;

  $logger->info("$key: variable names: $tmp");

  # prepare insert statement
  my $sql = "insert into $key ( $tmp ) values ( $placeholders )";
  $logger->info("sql: $sql");

  my $sth_add = $db2->prepare("$sql");

  $logger->info("$recs found in $key.  there are $numfields fields"); 
  my @a = ();
  my $i = 0;
  while ( @a = $sth->fetchrow_array()  ) { 
    $i++;
    $sth_add->execute(@a) ;
    if (! ($i % $trans) ) { 
       $db2->commit;   
       $logger->info("---- $key: added record total of $i");
    };
  };
  $db2->commit if ( ($i % $trans ) ); # commit if mod operation returns non-zero 
  $logger->info("---- $key: added record total of $i");
  $logger->info("Finished adding records to $key!  "); 
};
exit;

