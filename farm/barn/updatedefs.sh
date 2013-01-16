#/usr/local/bin/bash

# this file uses the SQL::Translate utility sqlt to transfer mysqldump generated
# definition files to sqlite3 table "main.db"
#
# after this process finishes, it is then possible to load these sqlite tables
# using the perl utility initial_load.pl
#

sqlt -F MySQL -t SQLite --show-warnings defs/apps.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/asnums.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/ccodes.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/classes.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/colors.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/disciplines.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/domains.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/govagencies.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/ips.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/ipstext.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/liveflow.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/macaddrs.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/orgclass.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/protocols.dump | sqlite3 main.db
sqlt -F MySQL -t SQLite --show-warnings defs/worldclass.dump | sqlite3 main.db


