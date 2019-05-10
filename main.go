package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	logsclient "github.com/SoftwareForScience/jiskefet-api-go/client/logs"
	"github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	runsclient "github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	tagsclient "github.com/SoftwareForScience/jiskefet-api-go/client/tags"
	"github.com/SoftwareForScience/jiskefet-api-go/models"
	"github.com/SoftwareForScience/jiskefet-migrate-logbook/logbook"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/client"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	_ "github.com/go-sql-driver/mysql"
)

type Args struct {
	username        string
	password        string
	logbookFilesDir string
	logbookDB       DBArgs
	jiskefetDB      DBArgs
	parallel        bool
	idOffset        int64
	runtime         *client.Runtime
	bearerToken     runtime.ClientAuthInfoWriter
}

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

func getLogbookSubsystems(logbookDB *sql.DB) []logbook.Subsystem {
	subsystems := make([]logbook.Subsystem, 0)
	{
		rows, err := logbookDB.Query("select * from logbook_subsystems")
		check(err)
		defer rows.Close()

		for rows.Next() {
			subsystems = append(subsystems, logbook.ScanSubsystem(rows))
		}
		check(rows.Err())
	}
	return subsystems
}

func getLogbookSubsystemsMap(logbookDB *sql.DB) map[int64]logbook.Subsystem {
	subsystems := make(map[int64]logbook.Subsystem)
	{
		rows, err := logbookDB.Query("select * from logbook_subsystems")
		check(err)
		defer rows.Close()

		for rows.Next() {
			row := logbook.ScanSubsystem(rows)
			subsystems[row.ID.Int64] = row
		}
		check(rows.Err())
	}
	return subsystems
}

/// Returns list of SubsystemIDs associated with the commentID
func getCommentSubsystems(commentID int64, logbookDB *sql.DB) []int64 {
	subIDs := make([]int64, 0)
	{
		rows, err := logbookDB.Query("select subsystemid from logbook_comments_subsystems where commentid=?", commentID)
		check(err)
		defer rows.Close()
		for rows.Next() {
			var subsystemID sql.NullInt64
			err := rows.Scan(&subsystemID)
			check(err)
			subIDs = append(subIDs, subsystemID.Int64)
		}
		check(rows.Err())
	}
	return subIDs
}

func migrateLogbookRuns(args Args, logbookDB *sql.DB, runBoundLower string, runBoundUpper string, queryLimit string) {
	// Initialize Jiskefet API
	client := runsclient.New(args.runtime, strfmt.Default)

	rows, err := logbookDB.Query("select * from logbook where run>=? and run<=? limit ?", runBoundLower, runBoundUpper, queryLimit)
	check(err)
	defer rows.Close()
	for rows.Next() {
		row := logbook.ScanRun(rows)

		//log.Println("run = " + row.run)
		//log.Printf("%+v\n", row)

		// Post data to Jiskefet
		startt, err := time.Parse(time.RFC3339, "2001-01-01T11:11:11Z")
		check(err)
		// endt, err := time.Parse(time.RFC3339, "2002-02-02T22:22:22Z")
		// check(err)

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

		_, err = client.PostRuns(params, args.bearerToken)
		check(err)
	}
	err = rows.Err()
	check(err)
}

/// Gets all the comments that are roots (i.e. don't have parents)
func getCommentRoots(logbookDB *sql.DB) []int64 {
	roots := make([]int64, 0)

	rows, err := logbookDB.Query("SELECT id FROM logbook_comments WHERE parent IS NULL")
	check(err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		err := rows.Scan(&id)
		check(err)
		roots = append(roots, id)
	}
	check(rows.Err())

	return roots
}

/// Gets all the comments that are children of the given root (i.e. belong to that thread)
func getThreadComments(rootID int64, logbookDB *sql.DB) []int64 {
	children := make([]int64, 0)

	rows, err := logbookDB.Query("SELECT id FROM logbook_comments WHERE root_parent = ?", rootID)
	check(err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		err := rows.Scan(&id)
		check(err)
		children = append(children, id)
	}
	check(rows.Err())

	return children
}

/// Make a map of a thread hierarchy
func getCommentParentChildrenMap(rootID int64, logbookDB *sql.DB) map[int64][]int64 {
	parentChildren := make(map[int64][]int64) // parent -> list of children

	rows, err := logbookDB.Query("select id,parent from logbook_comments")
	check(err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		var parentID sql.NullInt64
		err := rows.Scan(&id, &parentID)
		check(err)

		if parentID.Valid {
			parentChildren[parentID.Int64] = append(parentChildren[parentID.Int64], id)
		}
	}
	check(rows.Err())

	return parentChildren
}

func getComment(ID int64, logbookDB *sql.DB) logbook.Comment {
	rows, err := logbookDB.Query("select * from logbook_comments where id = ?", ID)
	check(err)
	defer rows.Close()

	var comment logbook.Comment
	for rows.Next() {
		comment = logbook.ScanComment(rows)
	}

	check(rows.Err())
	return comment
}

func getCommentFiles(ID int64, logbookDB *sql.DB) []logbook.File {
	files := make([]logbook.File, 0) // list of files
	rows, err := logbookDB.Query("SELECT * FROM logbook_files WHERE commentid = ?", ID)
	check(err)
	defer rows.Close()

	for rows.Next() {
		file := logbook.ScanFile(rows)
		files = append(files, file)
	}
	check(rows.Err())
	return files
}

func updateJiskefetLogCreationTime(ID int64, logbookTimeCreated string, jiskefetDB *sql.DB) {
	stmt, err := jiskefetDB.Prepare("UPDATE log SET creation_time=? WHERE log_id=?")
	check(err)
	_, err = stmt.Exec(logbookTimeCreated, ID)
	check(err)
}

func linkTagToLog(logID int64, tagText string, client *tagsclient.Client, auth *runtime.ClientAuthInfoWriter,
	tagIDCache *map[string]int64, tagIDCacheMutex *sync.Mutex) {

	var tagID int64
	func() {
		defer (*tagIDCacheMutex).Unlock()
		(*tagIDCacheMutex).Lock()
		if _, exists := (*tagIDCache)[tagText]; !exists {
			// Tag doesn't exist in cache
			// Check if tag exists in Jiskefet
			params := tagsclient.NewGetTagsParams()
			params.TagText = &tagText
			response, err := client.GetTags(params, *auth)
			check(err)
			resp := response.Payload.(map[string]interface{})
			data := resp["data"].(map[string]interface{})
			items := data["items"].([]interface{})
			if len(items) > 0 {
				// Tag exists, add ID to cache
				item := items[0].(map[string]interface{})
				tagID, err = item["tagId"].(json.Number).Int64()
				check(err)
			} else {
				// If not, add tag to Jiskefet and ID to cache
				params := tagsclient.NewPostTagsParams()
				params.CreateTagDto = new(models.CreateTagDto)
				params.CreateTagDto.TagText = &tagText
				response, err := client.PostTags(params, *auth)
				resp := response.Payload.(map[string]interface{})
				data := resp["data"].(map[string]interface{})
				item := data["item"].(map[string]interface{})
				tagID, err = item["tagId"].(json.Number).Int64()
				check(err)
				log.Printf("Tag %s did not exist, added to Jiskefet with ID=%d", tagText, tagID)
			}
			(*tagIDCache)[tagText] = tagID
		} else {
			tagID = (*tagIDCache)[tagText]
		}
	}()

	// Add it to the log
	params := tagsclient.NewPatchTagsIDLogsParams()
	params.ID = tagID
	params.LinkLogToTagDto = new(models.LinkLogToTagDto)
	params.LinkLogToTagDto.LogID = &logID
	// Disabled error check for now because status 200 is apparently seen as an error when it's actually fine
	//  _, err := tagsClient.PatchTagsIDLogs(params, auth)
	// check(err)
	client.PatchTagsIDLogs(params, *auth)
}

func migrateLogbookComments(args Args, logbookDB *sql.DB, jiskefetDB *sql.DB) {
	// Initialize Jiskefet API
	logsClient := logsclient.New(args.runtime, strfmt.Default)
	tagsClient := tagsclient.New(args.runtime, strfmt.Default)
	auth := args.bearerToken

	tagIDCache := make(map[string]int64) // Cache of tag text -> tag ID
	var tagIDCacheMutex = sync.Mutex{}

	// Get Comment data from DB
	log.Printf("Importing logbook_comments\n")
	roots := getCommentRoots(logbookDB) // IDs of thread roots

	// Get subsystems, to translate into tags
	subsystemsMap := getLogbookSubsystemsMap(logbookDB) // Use for tag names, logging only

	var wg sync.WaitGroup
	wg.Add(len(roots))

	log.Printf("Posting comments\n")
	for i, logbookRootID := range roots {
		parentChildren := getCommentParentChildrenMap(logbookRootID, logbookDB) // Get hierarchy map of this thread

		f := func(i int, logbookID int64) {
			defer wg.Done()
			// Recursion function to traverse parent -> child relations
			var Recurse func(logbookID int64, level int, jiskefetParentID int64, jiskefetRootID int64)
			Recurse = func(logbookID int64, level int, jiskefetParentID int64, jiskefetRootID int64) {
				comment := getComment(logbookID, logbookDB)

				// POST comment log
				log.Printf("Thread #%d\n", i+1)
				log.Printf("Logbook.ID=%d, Jiskefet.parentID=%d, Depth=%d ", logbookID, jiskefetParentID, level)

				jiskefetID := int64(-1)
				if level == 0 {
					// Necessary workaround for now... roots can only be runs
					// Post comment to root
					// run := int64(0)
					origin := "human"
					subtype := "run"
					params := logsclient.NewPostLogsParams()
					params.CreateLogDto = new(models.CreateLogDto)
					params.CreateLogDto.Attachments = make([]string, 0)
					params.CreateLogDto.Body = &comment.Comment.String
					params.CreateLogDto.Origin = &origin
					params.CreateLogDto.Subtype = &subtype
					params.CreateLogDto.Title = &comment.Title.String
					params.CreateLogDto.User = &comment.UserID.Int64
					response, err := logsClient.PostLogs(params, auth)
					check(err)

					// Get ID of POSTed log
					resp := response.Payload.(map[string]interface{})
					data := resp["data"].(map[string]interface{})
					item := data["item"].(map[string]interface{})
					id, err := item["logId"].(json.Number).Int64()
					// jiskefetID := *response.Payload.LogID // Use this once response schema is fixed
					check(err)
					jiskefetID = id
				} else {
					// Post comment to root
					// run := int64(0)
					origin := "human"
					subtype := "comment"
					params := logsclient.NewPostLogsThreadsParams()
					params.CreateCommentDto = new(models.CreateCommentDto)
					params.CreateCommentDto.Attachments = make([]string, 0)
					params.CreateCommentDto.Body = &comment.Comment.String
					params.CreateCommentDto.Origin = &origin
					params.CreateCommentDto.ParentID = &jiskefetParentID
					params.CreateCommentDto.RootID = &jiskefetRootID
					params.CreateCommentDto.Subtype = &subtype
					params.CreateCommentDto.Title = &comment.Title.String
					params.CreateCommentDto.User = &comment.UserID.Int64
					response, err := logsClient.PostLogsThreads(params, auth)
					check(err)

					// Get ID of POSTed log
					resp := response.Payload.(map[string]interface{})
					data := resp["data"].(map[string]interface{})
					item := data["item"].(map[string]interface{})
					id, err := item["logId"].(json.Number).Int64()
					// jiskefetID := *response.Payload.LogID // Use this once response schema is fixed
					check(err)
					jiskefetID = id
				}

				log.Printf("Jiskefet.ID=%d\n", jiskefetID)

				log.Printf("Updating creation time\n")
				updateJiskefetLogCreationTime(jiskefetID, comment.TimeCreated.String, jiskefetDB)

				log.Printf("Linking comment type tag\n")
				{
					// Add type tags to replace enum('GENERAL','HARDWARE','CAVERN','DQM/QA','SOFTWARE','NETWORK','EOS','DCS','OTHER')
					tagText := "COMMENT_TYPE/" + comment.CommentType.String
					log.Printf("Tag \"%s\"\n", tagText)
					linkTagToLog(jiskefetID, tagText, tagsClient, &auth, &tagIDCache, &tagIDCacheMutex)
				}

				log.Printf("Linking subsystem tag(s)\n")
				{
					subsystemIDs := getCommentSubsystems(logbookID, logbookDB)
					for _, subsystemID := range subsystemIDs {
						tagText := subsystemsMap[subsystemID].Name.String
						log.Printf("Tag \"%s\"\n", tagText)
						linkTagToLog(jiskefetID, tagText, tagsClient, &auth, &tagIDCache, &tagIDCacheMutex)
					}
				}

				// Get Files from DB (note: doesn't contain the actual file, it's just metadata)
				// log.Printf("Importing logbook_files\n")
				files := getCommentFiles(logbookID, logbookDB)
				if len(files) > 0 {
					// Post attachments to log
					log.Printf("Uploading %d attachments\n", len(files))
					for _, file := range files {
						log.Printf("File \"%s\" (%.0f kB)\n", file.FileName.String, float64(file.Size.Int64)/1024.0)
						uploadAttachment(args, jiskefetID, file, logsClient, auth)
					}
				}

				logbookChildrenIDs := parentChildren[logbookID]
				for _, logbookChildID := range logbookChildrenIDs {
					if level == 0 {
						// If we're the root, our ID is the parent and root for the child
						jiskefetParentID := jiskefetID
						jiskefetRootID := jiskefetID
						Recurse(logbookChildID, level+1, jiskefetParentID, jiskefetRootID)
					} else {
						// If we're a child, we're parent to our child, but root stays the same
						jiskefetParentID := jiskefetID
						Recurse(logbookChildID, level+1, jiskefetParentID, jiskefetRootID)
					}
				}
			}

			// Start off recursion for this thread root
			Recurse(logbookID, 0, -1, -1)
		}
		if args.parallel {
			go f(i, logbookRootID)
		} else {
			f(i, logbookRootID)
		}
	}
	wg.Wait()
}

func uploadAttachment(args Args, logID int64, file logbook.File, client *logsclient.Client, auth runtime.ClientAuthInfoWriter) {
	timeCreated := file.TimeCreated.String
	timeSplit := strings.Split(timeCreated, "-")
	year := timeSplit[0]
	month := timeSplit[1]

	fileNameSplit := strings.Split(file.FileName.String, ".")
	extension := fileNameSplit[len(fileNameSplit)-1]

	path := fmt.Sprintf("%s/%s-%s/%d_%d.%s",
		args.logbookFilesDir, year, month, file.CommentID.Int64, file.FileID.Int64, extension)

	log.Printf("Reading from \"%s\"", path)
	fileBytes, err := ioutil.ReadFile(path)
	check(err)

	mime := file.ContentType.String
	fileEncoded := base64.StdEncoding.EncodeToString([]byte(fileBytes))
	creationTimeT, err := time.Parse(time.RFC3339, "2001-01-01T11:11:11Z")
	check(err)
	creationTime := strfmt.DateTime(creationTimeT)

	// log.Printf("        WARNING: Attachments disabled, work in progress..\n")
	if mime == "image/jpeg" {
		log.Printf("WARNING: Skipping jpeg image due to server bug\n")
		return
	}
	if len(fileBytes) >= 8000 {
		log.Printf("WARNING: Skipping large file (8kB+) due to server bug\n")
		return
	}
	params := logsclient.NewPostLogsIDAttachmentsParams()
	params.CreateAttachmentDto = new(models.CreateAttachmentDto)
	params.CreateAttachmentDto.CreationTime = &creationTime
	params.CreateAttachmentDto.FileData = &fileEncoded
	params.CreateAttachmentDto.FileMime = &mime
	params.CreateAttachmentDto.FileName = &file.FileName.String
	params.CreateAttachmentDto.Title = file.Title.String
	params.ID = logID
	_, err = client.PostLogsIDAttachments(params, auth)
	check(err)
}

func migrateLogbookSubsystems(args Args, logbookDB *sql.DB, jiskefetDB *sql.DB) {
	// Unfortunately, we can't use the API for this, and need direct DB
	// access.

	// Get Subsystems
	logbookSubsystems := getLogbookSubsystems(logbookDB)
	// log.Printf("Logbook subsystems:\n%+v\n", logbookSubsystems)

	// Insert them into Jiskefet
	for _, subsystem := range logbookSubsystems {
		log.Printf("Inserting \"%s\":", subsystem.Name.String)
		stmt, err := jiskefetDB.Prepare("INSERT IGNORE INTO sub_system(subsystem_id, subsystem_name) VALUES(?,?)")
		check(err)
		res, err := stmt.Exec(subsystem.ID.Int64, subsystem.Name.String)
		check(err)
		lastID, err := res.LastInsertId()
		check(err)
		rowCnt, err := res.RowsAffected()
		check(err)
		if rowCnt == 0 {
			log.Printf("Not inserted, possible duplicate\n")
		} else {
			log.Printf("Inserted ID %d, affected %d\n", lastID, rowCnt)
		}
	}
}

func migrateLogbookUsers(args Args, logbookDB *sql.DB, jiskefetDB *sql.DB) {
	// Unfortunately, we can't use the API for this, and need direct DB
	// access.

	// Get Logbook users
	logbookUsers := make([]logbook.User, 0)
	func() {
		rows, err := logbookDB.Query("select * from logbook_users")
		check(err)
		defer rows.Close()

		for rows.Next() {
			logbookUsers = append(logbookUsers, logbook.ScanUser(rows))
		}
		check(rows.Err())
	}()
	//log.Printf("Logbook users:\n%+v\n", logbookUsers)

	// Insert them into Jiskefet
	for _, user := range logbookUsers {
		log.Printf("Inserting \"%d\"\n", user.ID.Int64)
		stmt, err := jiskefetDB.Prepare("INSERT IGNORE INTO user(user_id, external_id, sams_id) VALUES(?,?,?)")
		check(err)
		res, err := stmt.Exec(user.ID.Int64, user.ID.Int64, user.ID.Int64)
		check(err)
		lastID, err := res.LastInsertId()
		check(err)
		rowCnt, err := res.RowsAffected()
		check(err)
		if rowCnt == 0 {
			log.Printf("Not inserted, possible duplicate\n")
		} else {
			log.Printf("ID %d, affected %d\n", lastID, rowCnt)
		}
	}
}

func openDB(args DBArgs) *sql.DB {
	connectionString := args.userName + ":" + args.password + "@tcp(" + args.hostPort + ")/" + args.dbName
	connectionStringNoPass := args.userName + ":" + "****" + "@tcp(" + args.hostPort + ")/" + args.dbName
	log.Printf("Opening DB @ \"%s\"\n", connectionStringNoPass)
	db, err := sql.Open("mysql", connectionString)
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

func checkJiskefetConnection(args Args) {
	logsClient := logsclient.New(args.runtime, strfmt.Default)
	log.Printf("  Getting logs\n")
	params := logsclient.NewGetLogsParams()
	response, err := logsClient.GetLogs(params, args.bearerToken)
	log.Printf("  %+v\n", response)
	check(err)
}

func main() {
	idOffset := flag.Int64("idoffset", 1000000000, "IDs of logbook origin will get this offset in the jiskefet DB")
	queryLimit := flag.String("rlimit", "10", "Runs: Query result size limit")
	runBoundLower := flag.String("rmin", "500", "Runs: Lower run number bound")
	runBoundUpper := flag.String("rmax", "9999999", "Runs: Upper run number bound")
	parallel := flag.Bool("parallel", false, "Use parallel requests")
	tlsInsecureSkipVerify := flag.Bool("tlsskipverify", false, "Skip insecure TLS verification")

	checkOnly := flag.Bool("check", false, "Run a connectivity check and exit")
	migrateSubsystems := flag.Bool("msubsystems", false, "Migrate subsystems as subsystems & subsystem tags")
	migrateUsers := flag.Bool("musers", false, "Migrate users")
	migrateComments := flag.Bool("mcomments", false, "Migrate comments w. attachments & subsystem tags")
	migrateRuns := flag.Bool("mruns", false, "Migrate runs")
	flag.Parse()

	var args Args
	args.parallel = *parallel
	args.idOffset = *idOffset
	args.runtime = httptransport.New(os.Getenv("JISKEFET_HOST"), os.Getenv("JISKEFET_PATH"), nil)
	args.runtime.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: *tlsInsecureSkipVerify}}

	args.bearerToken = httptransport.BearerToken(os.Getenv("JISKEFET_API_TOKEN"))
	args.logbookFilesDir = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_FILESDIR")

	args.jiskefetDB.dbName = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_DBNAME")
	args.jiskefetDB.hostPort = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_HOSTPORT")
	args.jiskefetDB.userName = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_USERNAME")
	args.jiskefetDB.password = os.Getenv("JISKEFET_MIGRATE_JISKEFETDB_PASSWORD")

	args.logbookDB.dbName = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_DBNAME")
	args.logbookDB.hostPort = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_HOSTPORT")
	args.logbookDB.userName = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_USERNAME")
	args.logbookDB.password = os.Getenv("JISKEFET_MIGRATE_LOGBOOKDB_PASSWORD")

	log.Printf("Opening Logbook database\n")
	logbookDB := openDB(args.logbookDB)
	defer logbookDB.Close()
	log.Printf("Opening Jiskefet database\n")
	jiskefetDB := openDB(args.jiskefetDB)
	defer jiskefetDB.Close()

	if *checkOnly {
		log.Printf("Checking Jiskefet connection\n")
		checkJiskefetConnection(args)
		return
	}

	if *migrateSubsystems {
		log.Printf("Migrating subsystems...\n")
		migrateLogbookSubsystems(args, logbookDB, jiskefetDB)
	}

	if *migrateUsers {
		log.Printf("Migrating users...\n")
		migrateLogbookUsers(args, logbookDB, jiskefetDB)
	}

	if *migrateComments {
		log.Printf("Migrating comments...\n")
		migrateLogbookComments(args, logbookDB, jiskefetDB)
	}

	if *migrateRuns {
		log.Printf("Migrating runs...\n")
		migrateLogbookRuns(args, logbookDB, *runBoundLower, *runBoundUpper, *queryLimit)
	}
}
