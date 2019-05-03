# Jiskefet Migrate Logbook
This is a tool for reading data from the old logbook database format and sending it via the Jiskefet Go API.
It also needs direct access to the Jiskefet DB for migrating users.
Note that the migration of runs, comments, and attachments are not idempotent: migrate multiple times and you'll have duplicates.

## Setup
```
go get -u -v github.com/SoftwareForScience/jiskefet-migrate-logbook
```


## Running the example code
If your api is at `http://myhost.server.address/api`

```
cd $GOPATH/src/github.com/SoftwareForScience/jiskefet-migrate-logbook

export JISKEFET_HOST="myhost.server.address"
export JISKEFET_PATH="api"
export JISKEFET_API_TOKEN="sdfsakafuih32784h342..."

export JISKEFET_MIGRATE_JISKEFETDB_DBNAME="jiskefetdb"
export JISKEFET_MIGRATE_JISKEFETDB_HOSTPORT="192.168.122.235:3306"
export JISKEFET_MIGRATE_JISKEFETDB_USERNAME="user"
export JISKEFET_MIGRATE_JISKEFETDB_PASSWORD="pass"

export JISKEFET_MIGRATE_LOGBOOKDB_DBNAME="LOGBOOK_ITSRUN3"
export JISKEFET_MIGRATE_LOGBOOKDB_HOSTPORT="127.0.0.1:3306"
export JISKEFET_MIGRATE_LOGBOOKDB_USERNAME="user"
export JISKEFET_MIGRATE_LOGBOOKDB_PASSWORD="pass"
export JISKEFET_MIGRATE_LOGBOOKDB_FILESDIR="/home/user/logbook/fileAttachments"

go run main.go -h
```