package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	runsclient "github.com/SoftwareForScience/jiskefet-api-go/client/runs"
	"github.com/SoftwareForScience/jiskefet-api-go/models"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	_ "github.com/go-sql-driver/mysql"
)

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

func main() {
	hostUrl := os.Getenv("JISKEFET_HOST")
	apiPath := flag.String("apipath", "api", "Path to API")
	apiToken := os.Getenv("JISKEFET_API_TOKEN")
	username := os.Getenv("JISKEFET_MIGRATE_USER")
	password := os.Getenv("JISKEFET_MIGRATE_PASSWORD")
	databaseName := flag.String("db", "LOGBOOK", "Name of database")
	queryLimit := flag.String("limit", "10", "Query result size limit")
	runBoundLower := flag.String("rmin", "500", "Lower run number bound")
	runBoundUpper := flag.String("rmax", "9999999", "Upper run number bound")
	flag.Parse()

	// Initialize Jiskefet API
	client := runsclient.New(httptransport.New(hostUrl, *apiPath, nil), strfmt.Default)
	bearerTokenAuth := httptransport.BearerToken(apiToken)

	// Initialize SQL API
	db, err := sql.Open("mysql", username+":"+password+"@tcp(127.0.0.1:3306)/"+*databaseName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// Check if connection is OK
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

	rows, err := db.Query("select * from logbook where run>=? and run<=? limit ?", runBoundLower, runBoundUpper, queryLimit)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var row LogbookRow
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
		}
		//log.Println("run = " + row.run)
		//fmt.Printf("%+v\n", row)

		// Post data to Jiskefet
		startt, err := time.Parse(time.RFC3339, "2001-01-01T11:11:11Z")
		if err != nil {
			fmt.Println(err)
		}
		endt, err := time.Parse(time.RFC3339, "2002-02-02T22:22:22Z")
		if err != nil {
			fmt.Println(err)
		}

		start := strfmt.DateTime(startt)
		end := strfmt.DateTime(endt)
		runType := "my-run-type"
		runQuality := "my-run-quality"
		activityId := "migrate"
		nSubtimeframes := int64(12312)
		bytesTimeframeBuilder := int64(512 * 1024)

		params := runs.NewPostRunsParams()
		params.CreateRunDto = new(models.CreateRunDto)
		params.CreateRunDto.TimeO2Start = &start
		params.CreateRunDto.TimeTrgStart = &start
		params.CreateRunDto.TimeO2End = &end
		params.CreateRunDto.TimeTrgEnd = &end
		params.CreateRunDto.RunType = &runType
		params.CreateRunDto.RunQuality = &runQuality
		params.CreateRunDto.ActivityID = &activityId
		params.CreateRunDto.NDetectors = &row.numberOfDetectors.Int64
		params.CreateRunDto.NFlps = &row.numberOfLDCs.Int64
		params.CreateRunDto.NEpns = &row.numberOfGDCs.Int64
		params.CreateRunDto.NTimeframes = &row.totalEvents.Int64
		params.CreateRunDto.NSubtimeframes = &nSubtimeframes
		params.CreateRunDto.BytesReadOut = &row.totalDataRecorded.Int64
		params.CreateRunDto.BytesTimeframeBuilder = &bytesTimeframeBuilder

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
