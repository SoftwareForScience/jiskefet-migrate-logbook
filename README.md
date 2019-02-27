# Jiskefet Migrate Logbook
This is a tool for reading data from the old logbook database format and sending it via the Jiskefet Go API.


## Setup
```
go get -u -v github.com/SoftwareForScience/jiskefet-migrate-logbook
```


## Running the example code
If your api is at `http://myhost.server.address/api`

```
cd $GOPATH/src/github.com/SoftwareForScience/jiskefet-migrate-logbook
export JISKEFET_HOST=myhost.server.address
export JISKEFET_API_TOKEN=jnk5vh43785ycj4gdvlvm84fg...
export JISKEFET_MIGRATE_USER=database-user
export JISKEFET_MIGRATE_PASSWORD=database-password
go run main.go -h
```