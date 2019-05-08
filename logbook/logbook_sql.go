package logbook

import (
	"database/sql"
	"log"
)

func check(err error) {
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

// ScanRun ...
func ScanRun(rows *sql.Rows) Run {
	var row Run
	err := rows.Scan(
		&row.Run,
		&row.Time_created,
		&row.DAQ_time_start,
		&row.DAQ_time_end,
		&row.TRGTimeStart,
		&row.TRGTimeEnd,
		&row.Time_update,
		&row.RunDuration,
		&row.PauseDuration,
		&row.Partition,
		&row.Detector,
		&row.Run_type,
		&row.Calibration,
		&row.BeamEnergy,
		&row.BeamType,
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
		&row.ForceLHCReco,
		&row.NumberOfDetectors,
		&row.DetectorMask,
		&row.SplitterDetectorMask,
		&row.Log,
		&row.TotalSubEvents,
		&row.TotalDataReadout,
		&row.TotalEvents,
		&row.TotalEventsPhysics,
		&row.TotalEventsCalibration,
		&row.TotalEventsIncomplete,
		&row.TotalDataEventBuilder,
		&row.TotalDataRecorded,
		&row.AverageDataRateReadout,
		&row.AverageDataRateEventBuilder,
		&row.AverageDataRateRecorded,
		&row.AverageSubEventsPerSecond,
		&row.AverageEventsPerSecond,
		&row.NumberOfLDCs,
		&row.NumberOfGDCs,
		&row.NumberOfStreams,
		&row.LHCperiod,
		&row.HLTmode,
		&row.LDClocalRecording,
		&row.GDClocalRecording,
		&row.GDCmStreamRecording,
		&row.EventBuilding,
		&row.Time_completed,
		&row.Ecs_success,
		&row.Daq_success,
		&row.Eor_reason,
		&row.DataMigrated,
		&row.RunQuality,
		&row.L3_magnetCurrent,
		&row.Dipole_magnetCurrent,
		&row.L2a,
		&row.CtpDuration,
		&row.Ecs_iteration_current,
		&row.Ecs_iteration_total,
		&row.TotalNumberOfFilesWriting,
		&row.TotalNumberOfFilesClosed,
		&row.TotalNumberOfFilesWaitingMigration,
		&row.TotalNumberOfFilesMigrationRequested,
		&row.TotalNumberOfFilesMigrating,
		&row.TotalNumberOfFilesMigrated,
		&row.NumberOfPar,
		&row.NumberOfFailedPar)
	check(err)
	return row
}

/// ScanComment ...
func ScanComment(rows *sql.Rows) Comment {
	var row Comment
	err := rows.Scan(
		&row.ID,
		&row.Run,
		&row.UserID,
		&row.Title,
		&row.Comment,
		&row.Class,
		&row.CommentType,
		&row.TimeCreated,
		&row.Deleted,
		&row.Parent,
		&row.RootParent,
		&row.Dashboard,
		&row.TimeValidity,
		&row.ProcessedEmailNotification,
		&row.Context)
	check(err)
	return row
}

/// ScanUser ...
func ScanUser(rows *sql.Rows) User {
	var row User
	err := rows.Scan(
		&row.ID,
		&row.Username,
		&row.FirstName,
		&row.FullName,
		&row.Email,
		&row.GroupName,
		&row.LastLogin)
	check(err)
	return row
}

/// ScanFile ...
func ScanFile(rows *sql.Rows) File {
	var row File
	err := rows.Scan(
		&row.CommentID,
		&row.FileID,
		&row.FileName,
		&row.Size,
		&row.Title,
		&row.ContentType,
		&row.TimeCreated,
		&row.Deleted)
	check(err)
	return row
}

/// ScanSubsystem ...
func ScanSubsystem(rows *sql.Rows) Subsystem {
	var row Subsystem
	err := rows.Scan(
		&row.ID,
		&row.Name,
		&row.Text,
		&row.Parent,
		&row.Email,
		&row.EmailProcess,
		&row.NotifyNoRunLogEntries,
		&row.NotifyRunLogEntries,
		&row.NotifyQualityFlags,
		&row.NotifyGlobalQualityFlags,
		&row.NotifyProcessLogEntries,
		&row.Obsolete)
	check(err)
	return row
}

/// ScanCommentSubsystems ...
func ScanCommentSubsystems(rows *sql.Rows) CommentSubsystems {
	var row CommentSubsystems
	err := rows.Scan(
		&row.CommentID,
		&row.SubsystemID)
	check(err)
	return row
}
