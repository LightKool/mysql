package binlog

type EventType byte

// binlog event type constants
// refer to https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/binlogEvent.h
const (
	unknownEvent EventType = iota
	startEventV3
	queryEvent
	stopEvent
	rotateEvent
	intvarEvent
	loadEvent
	slaveEvent
	createFileEvent
	appendBlockEvent
	execLoadEvent
	deleteFileEvent
	newLoadEvent
	randEvent
	userVarEvent
	formatDescriptionEvent
	xidEvent
	beginLoadQueryEvent
	executeLoadQueryEvent
	tableMapEvent
	preGaWriteRowsEvent
	preGaUpdateRowsEvent
	preGaDeleteRowsEvent
	writeRowsEventV1
	updateRowsEventV1
	deleteRowsEventV1
	incidentEvent
	heartbeatEvent
	ignorableEvent
	rowsQueryEvent
	writeRowsEventV2
	updateRowsEventV2
	deleteRowsEventV2
	gtidEvent
	anonymousGtidEvent
	previousGtidsEvent
	transactionContextEvent
	viewChangeEvent
	xaPrepareLogEvent
)

func (t EventType) String() string {
	switch t {
	case unknownEvent:
		return "UnknownEvent"
	case startEventV3:
		return "StartEventV3"
	case queryEvent:
		return "QueryEvent"
	case stopEvent:
		return "StopEvent"
	case rotateEvent:
		return "RotateEvent"
	case intvarEvent:
		return "IntvarEvent"
	case loadEvent:
		return "LoadEvent"
	case slaveEvent:
		return "SlaveEvent"
	case createFileEvent:
		return "CreateFileEvent"
	case appendBlockEvent:
		return "AppendBlockEvent"
	case execLoadEvent:
		return "ExecLoadEvent"
	case deleteFileEvent:
		return "DeleteFileEvent"
	case newLoadEvent:
		return "NewLoadEvent"
	case randEvent:
		return "RandEvent"
	case userVarEvent:
		return "UserVarEvent"
	case formatDescriptionEvent:
		return "FormatDescriptionEvent"
	case xidEvent:
		return "XidEvent"
	case beginLoadQueryEvent:
		return "BeginLoadQueryEvent"
	case executeLoadQueryEvent:
		return "ExecuteLoadQueryEvent"
	case tableMapEvent:
		return "TableMapEvent"
	case preGaWriteRowsEvent:
		return "PreGaWriteRowsEvent"
	case preGaUpdateRowsEvent:
		return "PreGaUpdateRowsEvent"
	case preGaDeleteRowsEvent:
		return "PreGaDeleteRowsEvent"
	case writeRowsEventV1:
		return "WriteRowsEventV1"
	case updateRowsEventV1:
		return "UpdateRowsEventV1"
	case deleteRowsEventV1:
		return "DeleteRowsEventV1"
	case incidentEvent:
		return "IncidentEvent"
	case heartbeatEvent:
		return "HeartbeatEvent"
	case ignorableEvent:
		return "IgnorableEvent"
	case rowsQueryEvent:
		return "RowsQueryEvent"
	case writeRowsEventV2:
		return "WriteRowsEventV2"
	case updateRowsEventV2:
		return "UpdateRowsEventV2"
	case deleteRowsEventV2:
		return "DeleteRowsEventV2"
	case gtidEvent:
		return "GtidEvent"
	case anonymousGtidEvent:
		return "AnonymousGtidEvent"
	case previousGtidsEvent:
		return "PreviousGtidsEvent"
	case transactionContextEvent:
		return "TransactionContextEvent"
	case viewChangeEvent:
		return "ViewChangeEvent"
	case xaPrepareLogEvent:
		return "XaPrepareLogEvent"
	default:
		return "UnknownEvent"
	}
}
