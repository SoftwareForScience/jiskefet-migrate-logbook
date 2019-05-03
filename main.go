package main

import (
	"database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	attachmentsclient "github.com/SoftwareForScience/jiskefet-api-go/client/attachments"
	"github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	runsclient "github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	"github.com/SoftwareForScience/jiskefet-api-go/models"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	_ "github.com/go-sql-driver/mysql"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type LogbookCommentsRow struct {
	id                         sql.NullInt64
	run                        sql.NullInt64
	userid                     sql.NullInt64
	title                      sql.NullString
	comment                    sql.NullString
	class                      sql.NullString // enum('HUMAN','PROCESS')
	commentType                sql.NullString // enum('GENERAL','HARDWARE','CAVERN','DQM/QA','SOFTWARE','NETWORK','EOS','DCS','OTHER')
	timeCreated                sql.NullString // timestamp
	deleted                    sql.NullInt64
	parent                     sql.NullInt64
	rootParent                 sql.NullInt64
	dashboard                  sql.NullInt64
	timeValidity               sql.NullString // timestamp
	processedEmailNotification sql.NullInt64
	context                    sql.NullString // enum('DEFAULT','QUALITYFLAG','GLOBALQUALITYFLAG','EORREASON')
}

func scanLogbookCommentsRow(rows *sql.Rows, row *LogbookCommentsRow) {
	err := rows.Scan(
		&row.id,
		&row.run,
		&row.userid,
		&row.title,
		&row.comment,
		&row.class,
		&row.commentType,
		&row.timeCreated,
		&row.deleted,
		&row.parent,
		&row.rootParent,
		&row.dashboard,
		&row.timeValidity,
		&row.processedEmailNotification,
		&row.context)
	if err != nil {
		log.Println(err)
		panic("scan failure")
	}
}

type LogbookFilesRow struct {
	commentid   sql.NullInt64
	filename    sql.NullString
	title       sql.NullString
	timeCreated sql.NullString // timestamp
}

type LogbookSubsystemsRow struct {
	id   sql.NullInt64
	name sql.NullString
}

type LogbookUsersRow struct {
	id         sql.NullInt64
	username   sql.NullString
	first_name sql.NullString
	full_name  sql.NullString
	email      sql.NullString
	group_name sql.NullString
	last_login sql.NullString // timestamp
}

type LogbookRow struct {
	run                                    sql.NullString
	time_created                           sql.NullFloat64
	DAQ_time_start                         sql.NullFloat64
	DAQ_time_end                           sql.NullFloat64
	TRGTimeStart                           sql.NullFloat64
	TRGTimeEnd                             sql.NullFloat64
	time_update                            sql.NullString
	runDuration                            sql.NullInt64
	pauseDuration                          sql.NullInt64
	partition                              sql.NullString
	detector                               sql.NullString
	run_type                               sql.NullString
	calibration                            sql.NullInt64
	beamEnergy                             sql.NullString
	beamType                               sql.NullString
	LHCBeamMode                            sql.NullString
	LHCFillNumber                          sql.NullString
	LHCTotalInteractingBunches             sql.NullString
	LHCTotalNonInteractingBunchesBeam1     sql.NullString
	LHCTotalNonInteractingBunchesBeam2     sql.NullString
	LHCBetaStar                            sql.NullString
	LHCFillingSchemeName                   sql.NullString
	LHCInstIntensityNonInteractingBeam1SOR sql.NullString
	LHCInstIntensityNonInteractingBeam1EOR sql.NullString
	LHCInstIntensityNonInteractingBeam1Avg sql.NullString
	LHCInstIntensityInteractingBeam1SOR    sql.NullString
	LHCInstIntensityInteractingBeam1EOR    sql.NullString
	LHCInstIntensityInteractingBeam1Avg    sql.NullString
	LHCInstIntensityNonInteractingBeam2SOR sql.NullString
	LHCInstIntensityNonInteractingBeam2EOR sql.NullString
	LHCInstIntensityNonInteractingBeam2Avg sql.NullString
	LHCInstIntensityInteractingBeam2SOR    sql.NullString
	LHCInstIntensityInteractingBeam2EOR    sql.NullString
	LHCInstIntensityInteractingBeam2Avg    sql.NullString
	LHCInfoStatus                          sql.NullString
	forceLHCReco                           sql.NullString
	numberOfDetectors                      sql.NullInt64
	detectorMask                           sql.NullString
	splitterDetectorMask                   sql.NullString
	log                                    sql.NullString
	totalSubEvents                         sql.NullInt64
	totalDataReadout                       sql.NullInt64
	totalEvents                            sql.NullInt64
	totalEventsPhysics                     sql.NullInt64
	totalEventsCalibration                 sql.NullInt64
	totalEventsIncomplete                  sql.NullInt64
	totalDataEventBuilder                  sql.NullInt64
	totalDataRecorded                      sql.NullInt64
	averageDataRateReadout                 sql.NullString
	averageDataRateEventBuilder            sql.NullString
	averageDataRateRecorded                sql.NullString
	averageSubEventsPerSecond              sql.NullString
	averageEventsPerSecond                 sql.NullString
	numberOfLDCs                           sql.NullInt64
	numberOfGDCs                           sql.NullInt64
	numberOfStreams                        sql.NullInt64
	LHCperiod                              sql.NullString
	HLTmode                                sql.NullString
	LDClocalRecording                      sql.NullString
	GDClocalRecording                      sql.NullString
	GDCmStreamRecording                    sql.NullString
	eventBuilding                          sql.NullString
	time_completed                         sql.NullString
	ecs_success                            sql.NullString
	daq_success                            sql.NullString
	eor_reason                             sql.NullString
	dataMigrated                           sql.NullString
	runQuality                             sql.NullString
	L3_magnetCurrent                       sql.NullString
	Dipole_magnetCurrent                   sql.NullString
	L2a                                    sql.NullString
	ctpDuration                            sql.NullString
	ecs_iteration_current                  sql.NullString
	ecs_iteration_total                    sql.NullString
	totalNumberOfFilesWriting              sql.NullInt64
	totalNumberOfFilesClosed               sql.NullInt64
	totalNumberOfFilesWaitingMigration     sql.NullInt64
	totalNumberOfFilesMigrationRequested   sql.NullInt64
	totalNumberOfFilesMigrating            sql.NullInt64
	totalNumberOfFilesMigrated             sql.NullInt64
	numberOfPar                            sql.NullInt64
	numberOfFailedPar                      sql.NullInt64
}

func scanLogbookRow(rows *sql.Rows, row *LogbookRow) {
	err := rows.Scan(
		&row.run,
		&row.time_created,
		&row.DAQ_time_start,
		&row.DAQ_time_end,
		&row.TRGTimeStart,
		&row.TRGTimeEnd,
		&row.time_update,
		&row.runDuration,
		&row.pauseDuration,
		&row.partition,
		&row.detector,
		&row.run_type,
		&row.calibration,
		&row.beamEnergy,
		&row.beamType,
		&row.LHCBeamMode,
		&row.LHCFillNumber,
		&row.LHCTotalInteractingBunches,
		&row.LHCTotalNonInteractingBunchesBeam1,
		&row.LHCTotalNonInteractingBunchesBeam2,
		&row.LHCBetaStar,
		&row.LHCFillingSchemeName,
		&row.LHCInstIntensityNonInteractingBeam1SOR,
		&row.LHCInstIntensityNonInteractingBeam1EOR,
		&row.LHCInstIntensityNonInteractingBeam1Avg,
		&row.LHCInstIntensityInteractingBeam1SOR,
		&row.LHCInstIntensityInteractingBeam1EOR,
		&row.LHCInstIntensityInteractingBeam1Avg,
		&row.LHCInstIntensityNonInteractingBeam2SOR,
		&row.LHCInstIntensityNonInteractingBeam2EOR,
		&row.LHCInstIntensityNonInteractingBeam2Avg,
		&row.LHCInstIntensityInteractingBeam2SOR,
		&row.LHCInstIntensityInteractingBeam2EOR,
		&row.LHCInstIntensityInteractingBeam2Avg,
		&row.LHCInfoStatus,
		&row.forceLHCReco,
		&row.numberOfDetectors,
		&row.detectorMask,
		&row.splitterDetectorMask,
		&row.log,
		&row.totalSubEvents,
		&row.totalDataReadout,
		&row.totalEvents,
		&row.totalEventsPhysics,
		&row.totalEventsCalibration,
		&row.totalEventsIncomplete,
		&row.totalDataEventBuilder,
		&row.totalDataRecorded,
		&row.averageDataRateReadout,
		&row.averageDataRateEventBuilder,
		&row.averageDataRateRecorded,
		&row.averageSubEventsPerSecond,
		&row.averageEventsPerSecond,
		&row.numberOfLDCs,
		&row.numberOfGDCs,
		&row.numberOfStreams,
		&row.LHCperiod,
		&row.HLTmode,
		&row.LDClocalRecording,
		&row.GDClocalRecording,
		&row.GDCmStreamRecording,
		&row.eventBuilding,
		&row.time_completed,
		&row.ecs_success,
		&row.daq_success,
		&row.eor_reason,
		&row.dataMigrated,
		&row.runQuality,
		&row.L3_magnetCurrent,
		&row.Dipole_magnetCurrent,
		&row.L2a,
		&row.ctpDuration,
		&row.ecs_iteration_current,
		&row.ecs_iteration_total,
		&row.totalNumberOfFilesWriting,
		&row.totalNumberOfFilesClosed,
		&row.totalNumberOfFilesWaitingMigration,
		&row.totalNumberOfFilesMigrationRequested,
		&row.totalNumberOfFilesMigrating,
		&row.totalNumberOfFilesMigrated,
		&row.numberOfPar,
		&row.numberOfFailedPar)
	if err != nil {
		log.Println(err)
		panic("scan failure")
	}
}

type Args struct {
	hostURL      string
	apiPath      string
	apiToken     string
	username     string
	password     string
	databaseName string
}

func migrateLogbookRuns(args Args, db *sql.DB, runBoundLower string, runBoundUpper string, queryLimit string) {
	// Initialize Jiskefet API
	client := runsclient.New(httptransport.New(args.hostURL, args.apiPath, nil), strfmt.Default)
	bearerTokenAuth := httptransport.BearerToken(args.apiToken)

	rows, err := db.Query("select * from logbook where run>=? and run<=? limit ?", runBoundLower, runBoundUpper, queryLimit)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var row LogbookRow
		scanLogbookRow(rows, &row)

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
		params.CreateRunDto.NDetectors = &row.numberOfDetectors.Int64
		params.CreateRunDto.NFlps = &row.numberOfLDCs.Int64
		params.CreateRunDto.NEpns = &row.numberOfGDCs.Int64

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

func migrateLogbookComments(args Args, db *sql.DB) {
	// Initialize Jiskefet API
	// client := logsclient.New(httptransport.New(args.hostURL, args.apiPath, nil), strfmt.Default)
	// bearerTokenAuth := httptransport.BearerToken(args.apiToken)

	rows, err := db.Query("select * from logbook_comments")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	comments := make(map[int64]LogbookCommentsRow) // Map for rows
	roots := make([]int64, 0)                      // IDs of thread roots
	parentChildren := make(map[int64][]int64)      // parent -> list of children

	for rows.Next() {
		var row LogbookCommentsRow
		scanLogbookCommentsRow(rows, &row)
		// fmt.Printf("%+v\n", row)

		id := row.id.Int64
		comments[id] = row
		if !row.parent.Valid && !row.rootParent.Valid {
			// We have a thread root
			roots = append(roots, id)
		}
		if row.parent.Valid {
			parentChildren[row.parent.Int64] = append(parentChildren[row.parent.Int64], id)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Thread roots:\n%+v\n", roots)
	fmt.Printf("Thread parent->children:\n%+v\n", parentChildren)

	for _, id := range roots {

		// Recursion function to traverse parent -> child relations
		var Recurse func(id int64, level int)
		Recurse = func(id int64, level int) {
			// POST comment
			// logs.NewPostCommentParams()...
			// _, err = client.PostLogs(params, bearerTokenAuth)
			// if err != nil {
			// 	fmt.Println(err)
			// }

			fmt.Printf("  %s|_ %d\n", strings.Repeat("  ", level), id)
			childrenIDs := parentChildren[id]
			for _, childID := range childrenIDs {
				Recurse(childID, level+1)
			}
		}

		print("  o thread\n")
		Recurse(id, 0)
	}
}

func uploadAttachment(args Args) {
	client := attachmentsclient.New(httptransport.New(args.hostURL, args.apiPath, nil), strfmt.Default)
	auth := httptransport.BearerToken(args.apiToken)

	fileBytes, err := ioutil.ReadFile("/tmp/test-photo.jpg")
	check(err)

	fmt.Printf("Read photo, %d bytes\n", len(fileBytes))

	mime := "image/jpeg"
	contentType := "content-type: " + mime
	fileEncoded := base64.StdEncoding.EncodeToString([]byte(fileBytes))
	fileName := "test-photo.jpg"
	fileSize := int64(len(fileBytes))
	logID := int64(1)
	title := "Test photo"

	params := attachmentsclient.NewPostAttachmentsParams()
	params.CreateAttachmentDto = new(models.CreateAttachmentDto)
	params.CreateAttachmentDto.ContentType = &contentType
	params.CreateAttachmentDto.FileData = &fileEncoded
	params.CreateAttachmentDto.FileMime = &mime
	params.CreateAttachmentDto.FileName = &fileName
	params.CreateAttachmentDto.FileSize = &fileSize
	params.CreateAttachmentDto.LogID = &logID
	params.CreateAttachmentDto.Title = &title

	_, err = client.PostAttachments(params, auth)
	check(err)
}

func main() {
	apiPath := flag.String("apipath", "api", "Path to API")
	databaseName := flag.String("db", "LOGBOOK", "Name of database")
	queryLimit := flag.String("limit", "10", "Query result size limit")
	runBoundLower := flag.String("rmin", "500", "Lower run number bound")
	runBoundUpper := flag.String("rmax", "9999999", "Upper run number bound")
	flag.Parse()
	var args Args
	args.hostURL = os.Getenv("JISKEFET_HOST")
	args.apiToken = os.Getenv("JISKEFET_API_TOKEN")
	args.username = os.Getenv("JISKEFET_MIGRATE_USER")
	args.password = os.Getenv("JISKEFET_MIGRATE_PASSWORD")
	args.apiPath = *apiPath
	args.databaseName = *databaseName

	print("Opening database\n")
	db, err := sql.Open("mysql", args.username+":"+args.password+"@tcp(127.0.0.1:3306)/"+args.databaseName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	// Check if connection is OK
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

	uploadAttachment(args)
	return
	print("Migrating logbook_comments\n")
	migrateLogbookComments(args, db)
	return
	migrateLogbookRuns(args, db, *runBoundLower, *runBoundUpper, *queryLimit)
}
