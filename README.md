# Jiskefet Migrate Logbook
This is a tool for reading data from the old logbook database format and sending it via the Jiskefet Go API.
It also needs direct access to the Jiskefet DB for migrating users, subsystems, and creation times.
Note that the migration of runs, comments, and attachments are not idempotent: migrate multiple times and you'll have duplicates.


## Setup
```
go get -u -v github.com/SoftwareForScience/jiskefet-migrate-logbook
```


## Migrating
Before executing the migration, you'll need to set some environment variables.
If your api is at `http://myhost.server.address/api`, for example:

```
## Jiskefet API variables
export JISKEFET_HOST="myhost.server.address"
export JISKEFET_PATH="api"
# Note: don't included the "bearer" part!
export JISKEFET_API_TOKEN="sdfsakafuih32784h342..."

## Variables for the Logbook database to migrate from
export JISKEFET_MIGRATE_LOGBOOKDB_DBNAME="LOGBOOK_ITSRUN3"
export JISKEFET_MIGRATE_LOGBOOKDB_HOSTPORT="127.0.0.1:3306"
export JISKEFET_MIGRATE_LOGBOOKDB_USERNAME="user"
export JISKEFET_MIGRATE_LOGBOOKDB_PASSWORD="pass"
# Path to directory containing attachments. Expected directory structure:
#   [JISKEFET_MIGRATE_LOGBOOKDB_FILESDIR]/[year]-[month]/[comment ID]_[file ID].[file extension]
# Example:
#   /home/user/logbook/fileAttachments/2019-02/2_1.jpg
export JISKEFET_MIGRATE_LOGBOOKDB_FILESDIR="/home/user/logbook/fileAttachments"

## Variables for the Jiskefet database, necessary for certain bits of the migration.
## At some point the API may have enough functionality that this will no longer be needed.
export JISKEFET_MIGRATE_JISKEFETDB_DBNAME="jiskefetdb"
export JISKEFET_MIGRATE_JISKEFETDB_HOSTPORT="192.168.122.235:3306"
export JISKEFET_MIGRATE_JISKEFETDB_USERNAME="user"
export JISKEFET_MIGRATE_JISKEFETDB_PASSWORD="pass"
```
You may want to put these in a file and `source` them as needed.

Running:
```
# Show options
cd $GOPATH/src/github.com/SoftwareForScience/jiskefet-migrate-logbook
go run main.go -h

# Do a connectivity check (just does a GET /logs)
go run main.go -check

# Migrate everything (except runs, not used and fully tested yet)
# Note that the program will always migrate in the order: subsystems, users, runs, comments
go run main.go -msubsystems -musers -mcomments
```