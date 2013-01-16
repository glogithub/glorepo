#!/usr/bin/perl
use warnings;
use strict;

use POE;
use POE::Component::Server::TCP;
use POE::Filter::Stream;
use POE::Filter::Reference;
use XML::Simple; # qw(:strict);
use POE::Component::IKC::Client;
use POE::Component::IKC::Responder;

use Log::Log4perl;
Log::Log4perl->init_and_watch("logfile.conf", 'HUP');
my $logger = Log::Log4perl->get_logger();


## begin CONFIG

my $SERVER_ALIAS = "GloTop";
my $PORT_NUMBER = 12545;


##############################################################################################
# this next mess sets up all the IKC servers that we're going to connect with
# and arranges to keep a global hash (%farm) in which to keep certain information
# about them (such as alive status)

# vars = name, host, port, startsession name, alive status, program name, pingwait
my $defs = qq {
GLORIADFlow:localhost:12546:start_gloriadflow_session:0:./topusers.pl:30
};

my %farm = ();

foreach $_ (split(/\n/,$defs)) {
  chomp;  
  next if (/^s*$/);    # go to next if record is blank
  my ($name,$host,$port,$startsession,$alive,$program,$pingwait) = split(":",$_);
  $logger->info("loop: $name,$host,$port,$startsession,$alive,$program");
  ($farm{$name}{host},
   $farm{$name}{port},
   $farm{$name}{startsession},
   $farm{$name}{alive},
   $farm{$name}{program},
   $farm{$name}{pingwait}) = ($host,$port,$startsession,$alive,$program,$pingwait);
};

$logger->debug("finished splitting defs - startsession = ", $farm{GLORIADFlow}{startsession});

# end of mess
##############################################################################################

my $ENDSTREAM = "<ENDXML>"; 

## end   CONFIG



POE::Component::IKC::Responder->spawn();
$logger->info("finished spawning ikc Responder client.  poe_kernel = $poe_kernel");

$poe_kernel->post('IKC', 'monitor', 'GLORIADFlow', 
    {register=>'register', 
     unregister=>'reconnect', 
     subscribe=>'subscribe',
     unsubscribe=>'unsubscribe' });

##############################################################################################
# now, set up each of the individual IKC server connections using information from above

foreach my $key (keys %farm) {
  connectIKCServer($key);
};

#
##############################################################################################
# procedure for setting up a new IKC server connection.  This is separated out
# so that we can cal it independently for an IKC server if we detect that one has
# gone away

sub connectIKCServer {
my ($servername) = @_;
my ($kernel) = $_[ KERNEL ];

my $host = $farm{$servername}{host};
my $port = $farm{$servername}{port};
my $connect = \$farm{$servername}{startsession};;

POE::Component::IKC::Client->spawn(
  host       => $farm{$servername}{host},
  port       => $farm{$servername}{port},
  name       => "Client$$",
  on_connect => \&$$connect,
  on_error => sub {
    $logger->info("having trouble connecting to $servername ... trying in 2 seconds ..");
    my $program = $farm{$servername}{program};
    $logger->info("relaunching $program ..");
    system("$program &") if ($program);

    sleep 2;
    connectIKCServer("$servername");

  }
) or $logger->error("IKC client $servername wouldn't spawn");

$logger->info("finished spawning ikc client $servername");

};
##############################################################################################





POE::Component::Server::TCP->new(
  Alias        => $SERVER_ALIAS,
  Port         => $PORT_NUMBER,
  ClientFilter => POE::Filter::Line->new(InputLiteral => "$ENDSTREAM"),
  ClientConnected => sub {
      $logger->info("got a connection from $_[HEAP]{remote_ip}");     
      $_[KERNEL]->yield( 'ClientWrite', "connected!");      
    },
  ClientDisconnected => \&ClientDisconnected,
  ClientInput => \&Dispatcher,
  InlineStates => {
     ClientWrite => \&ClientWrite,
     UpdateUsers => \&UpdateUsers,
     UpdateControl => \&UpdateControl,
  }
  ) or do {
    $logger->error("TCP Server would not spawn .. dying ..");
    die;
  };
$logger->info("finished spawning server on port $PORT_NUMBER");
  
$poe_kernel->run( );
exit 0;


sub start_gloriadflow_session {

  $logger->debug("just entered start_gloriadflow_session");

  POE::Session->create(
    inline_states => {

      _start => sub {
        my ($kernel) = $_[KERNEL];
        $kernel->alias_set('GloriadFlowClient');
        $logger->debug("in _start of POE::Session->create ");

        $kernel->delay('pinger', 0, 'GLORIADFlow' );   

        $kernel->yield('start_ikc');
       },

      _default => sub {
        $logger->debug("default called for POE::Session!!");
        }, 

      start_ikc => sub {
        my ($kernel) = $_[KERNEL];

      $logger->debug("I got to beginning of start_ikc ");
      # Call remote 'inc' 
         $kernel->post(
          'IKC', 'call',
          'poe://GLORIADFlow/CoreServices/topu',
          "",
          'poe:UpdateControl'
        );
      },

      pinger => \&pinger, 
 
      processPing => \&processPing,

      kill_everyone => sub {
        my ($kernel, $heap) = @_[KERNEL, HEAP];
        $heap->{shutdown} = 1;
        $kernel->post(IKC => 'shutdown');
      },

      _stop => sub { $logger->debug("Finished..."); exit(0) },
            
      
    }
  );

}

sub ClientDisconnected {
   my ($kernel, $session, $heap) = @_[KERNEL, SESSION, HEAP];
  $logger->debug("disconnecting client" . $session->ID );  
  foreach my $key (keys %$heap) {
  		delete $heap->{$key};
  };
  return;
}
  
sub Dispatcher {
  my ($kernel, $session, $heap, $xml ) = @_[ KERNEL, SESSION, HEAP, ARG0 ];

  $heap->{lasttime} = time;

#  print "xml = $xml\n";

  my $x = XMLin($xml);
  
#  print "msg = ", $x->{msg}, "\n";
 
  $logger->debug("in Dispatcher: x->msg = ", $x->{msg});
 
  my $outp = ""; 
  if ($x->{msg} =~ /PingTest/) { 
    $outp = "<dispatch><msg>PingTest</msg></dispatch>\n";
   $kernel->yield( 'ClientWrite', $outp);
 } elsif ($x->{msg} =~ /UpdateUsers/) {
    $kernel->yield( 'UpdateUsers', $xml);
 } elsif ($x->{msg} =~ /UpdateControl/) {
    $kernel->yield( 'UpdateControl', $xml);
  } else {
 #   $outp =  $xml ;
  $kernel->yield( 'ClientWrite', "$xml");
  };
#  print "$outp";
  };

sub UpdateUsers {
  my ($kernel, $session, $heap, $xml ) = @_[ KERNEL, SESSION, HEAP, ARG0 ];

  my $x = XMLin($xml);
  my $timer = $x->{parameters}->{sec} || "";
  
  my $oldtimer = $heap->{'updateusers.timer'} || "";
#   print "in UpdateUsers timer = $timer, oldtimer = $oldtimer \n";
  if (! $oldtimer) { $oldtimer = $timer; };  # takes care of first past thru
  
  if ($timer ge $oldtimer) {
  $heap->{'updateusers.timer'} = $timer;
          $kernel->post(
          'IKC', 'call',
          'poe://GLORIADFlow/CoreServices/topu',
          $xml,
          'poe:UpdateControl'
        );
     $kernel->delay_set('UpdateUsers', 5, $xml);
  };

};
  
sub UpdateControl {
  my ($kernel, $session, $heap, $xml ) = @_[ KERNEL, SESSION, HEAP, ARG0 ];

#print "---------------------------\nin ClientWrite - xml = $xml ---------------------------\n\n";
 # $heap->{client}->put($xml) if ($xml);

$kernel->yield( 'ClientWrite', "$xml");

};
  

sub ClientWrite {
  my ($kernel, $session, $heap, $msg ) = @_[ KERNEL, SESSION, HEAP, ARG0 ];
 
  $msg .= "\n$ENDSTREAM";
  if ($heap->{client}) {
  $heap->{client}->put($msg);
  my $dt = `date`;
  };

};
    
sub processPing {
   my($name,$ping) = @{ $_[ARG0] } ;
   my ($kernel, $session, $heap) = @_[KERNEL, SESSION, HEAP];
     
   $logger->debug("in processPing - name, ping = $name, $ping");
   if ($ping) {
     $farm{$name}{alive} = 1;
   } elsif ($farm{$name}{alive}) {
     $farm{$name}{alive} = 0;
     $poe_kernel->delay('pinger', $farm{$name}{pingwait}, $name );   
   } else {
     $logger->info("in processPing - need to get $name back operational .. ");
     $farm{$name}{alive} = 0;
     
     connectIKCServer($name);
    };
};

sub pinger {
  my ($kernel,$name) = @_[KERNEL, ARG0];

   $logger->debug("in pinger - name = $name");

   $farm{'$name'}{alive} = 0;  # assume dead
   $poe_kernel->delay('processPing', $farm{$name}{pingwait}, [ $name, 0 ] );   # timeout
   $logger->debug("in pinger - just did the delay processPing .. now posting alive .. name $name .. ");
   
   my $postee = "poe://$name/CoreServices/alive";
   
   $kernel->post('IKC', 'call', $postee, [$name], 'poe:processPing');
};



