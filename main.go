package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	attachmentsclient "github.com/SoftwareForScience/jiskefet-api-go/client/attachments"
	logsclient "github.com/SoftwareForScience/jiskefet-api-go/client/logs"
	"github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	runsclient "github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	"github.com/SoftwareForScience/jiskefet-api-go/models"
	"github.com/SoftwareForScience/jiskefet-migrate-logbook/logbook"
	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	_ "github.com/go-sql-driver/mysql"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type DBArgs struct {
	dbName   string
	hostPort string
	userName string
	password string
}

type Args struct {
	hostURL         string
	apiPath         string
	apiToken        string
	username        string
	password        string
	logbookFilesDir string
	logbookDB       DBArgs
	jiskefetDB      DBArgs
	parallel        bool
}

func migrateLogbookRuns(args Args, logbookDB *sql.DB, runBoundLower string, runBoundUpper string, queryLimit string) {
	// Initialize Jiskefet API
	client := runsclient.New(httptransport.New(args.hostURL, args.apiPath, nil), strfmt.Default)
	bearerTokenAuth := httptransport.BearerToken(args.apiToken)

	rows, err := logbookDB.Query("select * from logbook where run>=? and run<=? limit ?", runBoundLower, runBoundUpper, queryLimit)
	check(err)
	defer rows.Close()
	for rows.Next() {
		row := logbook.ScanRun(rows)

		//log.Println("run = " + row.run)
		//fmt.Printf("%+v\n", row)

		// Post data to Jiskefet
		startt, err := time.Parse(time.RFC3339, "2001-01-01T11:11:11Z")
		if err != nil {
			fmt.Println(err)
		}
		// endt, err := time.Parse(time.RFC3339, "2002-02-02T22:22:22Z")
		// if err != nil {
		// 	fmt.Println(err)
		// }

		start := strfmt.DateTime(startt)
		// end := strfmt.DateTime(endt)
		runType := "my-run-type"
		// runQuality := "my-run-quality"
		// activityId := "migrate"
		// nSubtimeframes := int64(12312)
		// bytesTimeframeBuilder := int64(512 * 1024)

		params := runs.NewPostRunsParams()
		params.CreateRunDto = new(models.CreateRunDto)
		params.CreateRunDto.O2StartTime = &start
		params.CreateRunDto.TrgStartTime = &start
		params.CreateRunDto.RunType = &runType
		// params.CreateRunDto.ActivityID = &activityId
		params.CreateRunDto.NDetectors = &row.NumberOfDetectors.Int64
		params.CreateRunDto.NFlps = &row.NumberOfLDCs.Int64
		params.CreateRunDto.NEpns = &row.NumberOfGDCs.Int64

		_, err = client.PostRuns(params, bearerTokenAuth)
		if err != nil {
			fmt.Println(err)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func migrateLogbookComments(args Args, logbookDB *sql.DB) {
	// Initialize Jiskefet API
	logsClient := logsclient.New(httptransport.New(args.hostURL, args.apiPath, nil), strfmt.Default)
	attachmentsClient := attachmentsclient.New(httptransport.New(args.hostURL, args.apiPath, nil), strfmt.Default)
	auth := httptransport.BearerToken(args.apiToken)

	// Get Comment data from DB and put into sensible data structures
	println("Importing logbook_comments")
	comments := make(map[int64]logbook.Comment) // Map for rows
	roots := make([]int64, 0)                   // IDs of thread roots
	parentChildren := make(map[int64][]int64)   // parent -> list of children
	{
		rows, err := logbookDB.Query("select * from logbook_comments")
		check(err)
		defer rows.Close()

		for rows.Next() {
			row := logbook.ScanComment(rows)

			id := row.ID.Int64
			comments[id] = row
			if !row.Parent.Valid && !row.RootParent.Valid {
				// We have a thread root
				roots = append(roots, id)
			}
			if row.Parent.Valid {
				parentChildren[row.Parent.Int64] = append(parentChildren[row.Parent.Int64], id)
			}
		}
		check(rows.Err())
	}
	// fmt.Printf("Thread roots:\n%+v\n", roots)
	// fmt.Printf("Thread parent->children:\n%+v\n", parentChildren)

	// Get Files from DB (note: doesn't contain the actual file, it's just metadata)
	println("Importing logbook_files")
	files := make(map[int64][]logbook.File) // comment_id -> list of files
	{
		rows, err := logbookDB.Query("select * from logbook_files")
		check(err)
		defer rows.Close()

		for rows.Next() {
			file := logbook.ScanFile(rows)
			files[file.CommentID.Int64] = append(files[file.CommentID.Int64], file)
		}
		check(rows.Err())
	}
	// fmt.Printf("Files:\n%+v\n", files)

	var wg sync.WaitGroup
	wg.Add(len(roots))

	println("Posting comments")
	for i, id := range roots {
		f := func(i int, id int64) {
			defer wg.Done()
			// Recursion function to traverse parent -> child relations
			var Recurse func(id int64, level int, parentLogID int64)
			Recurse = func(id int64, level int, parentLogID int64) {
				comment := comments[id]

				// POST comment log
				fmt.Printf("  - Comment #%d\n", i+1)
				fmt.Printf("    logbook ID = %d\n", id)
				runs := make([]string, 0)
				origin := "human"
				subtype := "comment"
				params := logsclient.NewPostLogsParams()
				params.CreateLogDto = new(models.CreateLogDto)
				params.CreateLogDto.Body = &comment.Comment.String
				params.CreateLogDto.Origin = &origin
				params.CreateLogDto.Runs = runs
				params.CreateLogDto.Subtype = &subtype
				params.CreateLogDto.Title = &comment.Title.String
				params.CreateLogDto.User = &comment.UserID.Int64
				//params.CreateLogDto.ParentLogID = parentLogID
				response, err := logsClient.PostLogs(params, auth)
				check(err)

				// Get ID of POSTed log
				resp := response.Payload.(map[string]interface{})
				data := resp["data"].(map[string]interface{})
				item := data["item"].(map[string]interface{})
				logID, err := item["logId"].(json.Number).Int64()
				check(err)
				fmt.Printf("    jiskefet ID = %d\n", logID)
				fmt.Printf("    jiskefet parent ID = %d\n", parentLogID)

				if _, exists := files[id]; exists {
					// Post attachments to log
					fmt.Printf("    Uploading %d attachments\n", len(files[id]))
					for _, file := range files[id] {
						fmt.Printf("      - File \"%s\" (%.0f kB)\n", file.FileName.String, float64(file.Size.Int64)/1024.0)
						uploadAttachment(args, logID, file, attachmentsClient, auth)
					}
				}

				//fmt.Printf("  %s|_ %d\n", strings.Repeat("  ", level), id)
				childrenIDs := parentChildren[id]
				for _, childID := range childrenIDs {
					Recurse(childID, level+1, logID)
				}
			}
			// print("  o thread\n")
			Recurse(id, 0, -1)
		}
		if args.parallel {
			go f(i, id)
		} else {
			f(i, id)
		}
	}
	wg.Wait()
}

func uploadAttachment(args Args, logID int64, file logbook.File, client *attachmentsclient.Client, auth runtime.ClientAuthInfoWriter) {
	timeCreated := file.TimeCreated.String
	timeSplit := strings.Split(timeCreated, "-")
	year := timeSplit[0]
	month := timeSplit[1]

	fileNameSplit := strings.Split(file.FileName.String, ".")
	extension := fileNameSplit[len(fileNameSplit)-1]

	path := fmt.Sprintf("%s/%s-%s/%d_%d.%s",
		args.logbookFilesDir, year, month, file.CommentID.Int64, file.FileID.Int64, extension)

	fmt.Printf("        Reading from \"%s\" ... ", path)
	fileBytes, err := ioutil.ReadFile(path)
	check(err)
	fmt.Printf("%d bytes\n", len(fileBytes))

	mime := file.ContentType.String
	fileEncoded := base64.StdEncoding.EncodeToString([]byte(fileBytes))

	params := attachmentsclient.NewPostAttachmentsParams()
	params.CreateAttachmentDto = new(models.CreateAttachmentDto)
	params.CreateAttachmentDto.ContentType = &mime
	params.CreateAttachmentDto.FileData = &fileEncoded
	params.CreateAttachmentDto.FileMime = &mime
	params.CreateAttachmentDto.FileName = &file.FileName.String
	params.CreateAttachmentDto.FileSize = &file.Size.Int64
	params.CreateAttachmentDto.LogID = &logID
	params.CreateAttachmentDto.Title = &file.Title.String

	println("        NOTE: PostAttachment disabled due to server bug")
	return
	_, err = client.PostAttachments(params, auth)
	check(err)
}

func migrateLogbookSubsystems(args Args, logbookDB *sql.DB, jiskefetDB *sql.DB) {
	// Unfortunately, we can't use the API for this, and need direct DB
	// access.

	// Get Subsystems
	logbookSubsystems := make([]logbook.Subsystem, 0)
	{
		rows, err := logbookDB.Query("select * from logbook_subsystems")
		check(err)
		defer rows.Close()

		for rows.Next() {
			logbookSubsystems = append(logbookSubsystems, logbook.ScanSubsystem(rows))
		}
		check(rows.Err())
	}
	// fmt.Printf("Logbook subsystems:\n%+v\n", logbookSubsystems)

	// Insert them into Jiskefet
	for _, subsystem := range logbookSubsystems {
		fmt.Printf("  - Inserting \"%s\"\n", subsystem.Name.String)
		stmt, err := jiskefetDB.Prepare("INSERT IGNORE INTO sub_system(subsystem_id, subsystem_name) VALUES(?,?)")
		check(err)
		res, err := stmt.Exec(subsystem.ID.Int64, subsystem.Name.String)
		check(err)
		lastID, err := res.LastInsertId()
		check(err)
		rowCnt, err := res.RowsAffected()
		check(err)
		if rowCnt == 0 {
			println("    Not inserted, possible duplicate")
		} else {
			fmt.Printf("    Inserted ID = %d, affected = %d\n", lastID, rowCnt)
		}
	}
}

func migrateLogbookUsers(args Args, logbookDB *sql.DB, jiskefetDB *sql.DB) {
	// Unfortunately, we can't use the API for this, and need direct DB
	// access.

	// Get Logbook users
	logbookUsers := make([]logbook.User, 0)
	{
		rows, err := logbookDB.Query("select * from logbook_users")
		check(err)
		defer rows.Close()

		for rows.Next() {
			logbookUsers = append(logbookUsers, logbook.ScanUser(rows))
		}
		check(rows.Err())
	}
	//fmt.Printf("Logbook users:\n%+v\n", logbookUsers)

	// Insert them into Jiskefet
	for _, user := range logbookUsers {
		fmt.Printf("  - Inserting \"%d\"\n", user.ID.Int64)
		stmt, err := jiskefetDB.Prepare("INSERT IGNORE INTO user(user_id, external_id, sams_id) VALUES(?,?,?)")
		check(err)
		res, err := stmt.Exec(user.ID.Int64, user.ID.Int64, user.ID.Int64)
		check(err)
		lastID, err := res.LastInsertId()
		check(err)
		rowCnt, err := res.RowsAffected()
		check(err)
		if rowCnt == 0 {
			println("    Not inserted, possible duplicate")
		} else {
			fmt.Printf("    ID = %d, affected = %d\n", lastID, rowCnt)
		}
	}
}

func openDB(args DBArgs) *sql.DB {
	db, err := sql.Open("mysql", args.userName+":"+args.password+"@tcp("+args.hostPort+")/"+args.dbName)
	if err != nil {
		db.Close()
		panic(err.Error())
	}
	// Check if connection is OK
	err = db.Ping()
	if err != nil {
		db.Close()
		panic(err.Error())
	}
	return db
}

func main() {
	queryLimit := flag.String("rlimit", "10", "Runs: Query result size limit")
	runBoundLower := flag.String("rmin", "500", "Runs: Lower run number bound")
	runBoundUpper := flag.String("rmax", "9999999", "Runs: Upper run number bound")
	parallel := flag.Bool("parallel", false, "Use parallel requests")

	migrateSubsystems := flag.Bool("msubsystems", false, "Migrate subsystems")
	migrateUsers := flag.Bool("musers", false, "Migrate users")
	migrateComments := flag.Bool("mcomments", false, "Migrate comments & attachments")
	migrateRuns := flag.Bool("mruns", false, "Migrate runs")
	flag.Parse()

	var args Args
	args.parallel = *parallel
	args.hostURL = os.Getenv("JISKEFET_HOST")
	args.apiPath = os.Getenv("JISKEFET_PATH")
	args.apiToken = os.Getenv("JISKEFET_API_TOKEN")
	args.logbookFilesDir = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_FILESDIR")

	args.jiskefetDB.dbName = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_DBNAME")
	args.jiskefetDB.hostPort = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_HOSTPORT")
	args.jiskefetDB.userName = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_USERNAME")
	args.jiskefetDB.password = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_PASSWORD")

	args.logbookDB.dbName = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_DBNAME")
	args.logbookDB.hostPort = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_HOSTPORT")
	args.logbookDB.userName = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_USERNAME")
	args.logbookDB.password = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_PASSWORD")

	print("Opening Logbook database\n")
	logbookDB := openDB(args.logbookDB)
	defer logbookDB.Close()
	print("Opening Jiskefet database\n")
	jiskefetDB := openDB(args.jiskefetDB)
	defer jiskefetDB.Close()

	if *migrateSubsystems {
		println("Migrating subsystems...")
		migrateLogbookSubsystems(args, logbookDB, jiskefetDB)
	}

	if *migrateUsers {
		println("Migrating users...")
		migrateLogbookUsers(args, logbookDB, jiskefetDB)
	}

	if *migrateComments {
		println("Migrating comments...")
		migrateLogbookComments(args, logbookDB)
	}

	if *migrateRuns {
		println("Migrating runs...")
		migrateLogbookRuns(args, logbookDB, *runBoundLower, *runBoundUpper, *queryLimit)
	}
}
