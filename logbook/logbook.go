package logbook

import "database/sql"

type Run struct {
	Run                                    sql.NullString
	Time_created                           sql.NullFloat64
	DAQ_time_start                         sql.NullFloat64
	DAQ_time_end                           sql.NullFloat64
	TRGTimeStart                           sql.NullFloat64
	TRGTimeEnd                             sql.NullFloat64
	Time_update                            sql.NullString
	RunDuration                            sql.NullInt64
	PauseDuration                          sql.NullInt64
	Partition                              sql.NullString
	Detector                               sql.NullString
	Run_type                               sql.NullString
	Calibration                            sql.NullInt64
	BeamEnergy                             sql.NullString
	BeamType                               sql.NullString
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
	ForceLHCReco                           sql.NullString
	NumberOfDetectors                      sql.NullInt64
	DetectorMask                           sql.NullString
	SplitterDetectorMask                   sql.NullString
	Log                                    sql.NullString
	TotalSubEvents                         sql.NullInt64
	TotalDataReadout                       sql.NullInt64
	TotalEvents                            sql.NullInt64
	TotalEventsPhysics                     sql.NullInt64
	TotalEventsCalibration                 sql.NullInt64
	TotalEventsIncomplete                  sql.NullInt64
	TotalDataEventBuilder                  sql.NullInt64
	TotalDataRecorded                      sql.NullInt64
	AverageDataRateReadout                 sql.NullString
	AverageDataRateEventBuilder            sql.NullString
	AverageDataRateRecorded                sql.NullString
	AverageSubEventsPerSecond              sql.NullString
	AverageEventsPerSecond                 sql.NullString
	NumberOfLDCs                           sql.NullInt64
	NumberOfGDCs                           sql.NullInt64
	NumberOfStreams                        sql.NullInt64
	LHCperiod                              sql.NullString
	HLTmode                                sql.NullString
	LDClocalRecording                      sql.NullString
	GDClocalRecording                      sql.NullString
	GDCmStreamRecording                    sql.NullString
	EventBuilding                          sql.NullString
	Time_completed                         sql.NullString
	Ecs_success                            sql.NullString
	Daq_success                            sql.NullString
	Eor_reason                             sql.NullString
	DataMigrated                           sql.NullString
	RunQuality                             sql.NullString
	L3_magnetCurrent                       sql.NullString
	Dipole_magnetCurrent                   sql.NullString
	L2a                                    sql.NullString
	CtpDuration                            sql.NullString
	Ecs_iteration_current                  sql.NullString
	Ecs_iteration_total                    sql.NullString
	TotalNumberOfFilesWriting              sql.NullInt64
	TotalNumberOfFilesClosed               sql.NullInt64
	TotalNumberOfFilesWaitingMigration     sql.NullInt64
	TotalNumberOfFilesMigrationRequested   sql.NullInt64
	TotalNumberOfFilesMigrating            sql.NullInt64
	TotalNumberOfFilesMigrated             sql.NullInt64
	NumberOfPar                            sql.NullInt64
	NumberOfFailedPar                      sql.NullInt64
}

type Comment struct {
	ID                         sql.NullInt64
	Run                        sql.NullInt64
	UserID                     sql.NullInt64
	Title                      sql.NullString
	Comment                    sql.NullString
	Class                      sql.NullString // enum('HUMAN','PROCESS')
	CommentType                sql.NullString // enum('GENERAL','HARDWARE','CAVERN','DQM/QA','SOFTWARE','NETWORK','EOS','DCS','OTHER')
	TimeCreated                sql.NullString // timestamp
	Deleted                    sql.NullInt64
	Parent                     sql.NullInt64
	RootParent                 sql.NullInt64
	Dashboard                  sql.NullInt64
	TimeValidity               sql.NullString // timestamp
	ProcessedEmailNotification sql.NullInt64
	Context                    sql.NullString // enum('DEFAULT','QUALITYFLAG','GLOBALQUALITYFLAG','EORREASON')
}

type File struct {
	CommentID   sql.NullInt64
	FileID      sql.NullInt64
	FileName    sql.NullString
	Size        sql.NullInt64
	Title       sql.NullString
	ContentType sql.NullString
	TimeCreated sql.NullString // timestamp
	Deleted     sql.NullInt64
}

type Subsystem struct {
	ID   sql.NullInt64
	Name sql.NullString
}

type User struct {
	ID        sql.NullInt64  // int(11)
	Username  sql.NullString // char(32)
	FirstName sql.NullString // char(32)
	FullName  sql.NullString // char(128)
	Email     sql.NullString // char(128)
	GroupName sql.NullString // char(16)
	LastLogin sql.NullString // timestamp
}
