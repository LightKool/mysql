package binlog

type EventType byte

// binlog event type constants
// refer to https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/binlogEvent.h
const (
	UnknownEventType EventType = iota
	StartEventTypeV3
	QueryEventType
	StopEventType
	RotateEventType
	IntvarEventType
	LoadEventType
	SlaveEventType
	CreateFileEventType
	AppendBlockEventType
	ExecLoadEventType
	DeleteFileEventType
	NewLoadEventType
	RandEventType
	UserVarEventType
	FormatDescriptionEventType
	XidEventType
	BeginLoadQueryEventType
	ExecuteLoadQueryEventType
	TableMapEventType
	PreGaWriteRowsEventType
	PreGaUpdateRowsEventType
	PreGaDeleteRowsEventType
	WriteRowsEventTypeV1
	UpdateRowsEventTypeV1
	DeleteRowsEventTypeV1
	IncidentEventType
	HeartbeatEventType
	IgnorableEventType
	RowsQueryEventType
	WriteRowsEventTypeV2
	UpdateRowsEventTypeV2
	DeleteRowsEventTypeV2
	GtidEventType
	AnonymousGtidEventType
	PreviousGtidsEventType
	TransactionContextEventType
	ViewChangeEventType
	XaPrepareLogEventType
)

func (t EventType) String() string {
	switch t {
	case UnknownEventType:
		return "UnknownEvent"
	case StartEventTypeV3:
		return "StartEventV3"
	case QueryEventType:
		return "QueryEvent"
	case StopEventType:
		return "StopEvent"
	case RotateEventType:
		return "RotateEvent"
	case IntvarEventType:
		return "IntvarEvent"
	case LoadEventType:
		return "LoadEvent"
	case SlaveEventType:
		return "SlaveEvent"
	case CreateFileEventType:
		return "CreateFileEvent"
	case AppendBlockEventType:
		return "AppendBlockEvent"
	case ExecLoadEventType:
		return "ExecLoadEvent"
	case DeleteFileEventType:
		return "DeleteFileEvent"
	case NewLoadEventType:
		return "NewLoadEvent"
	case RandEventType:
		return "RandEvent"
	case UserVarEventType:
		return "UserVarEvent"
	case FormatDescriptionEventType:
		return "FormatDescriptionEvent"
	case XidEventType:
		return "XidEvent"
	case BeginLoadQueryEventType:
		return "BeginLoadQueryEvent"
	case ExecuteLoadQueryEventType:
		return "ExecuteLoadQueryEvent"
	case TableMapEventType:
		return "TableMapEvent"
	case PreGaWriteRowsEventType:
		return "PreGaWriteRowsEvent"
	case PreGaUpdateRowsEventType:
		return "PreGaUpdateRowsEvent"
	case PreGaDeleteRowsEventType:
		return "PreGaDeleteRowsEvent"
	case WriteRowsEventTypeV1:
		return "WriteRowsEventV1"
	case UpdateRowsEventTypeV1:
		return "UpdateRowsEventV1"
	case DeleteRowsEventTypeV1:
		return "DeleteRowsEventV1"
	case IncidentEventType:
		return "IncidentEvent"
	case HeartbeatEventType:
		return "HeartbeatEvent"
	case IgnorableEventType:
		return "IgnorableEvent"
	case RowsQueryEventType:
		return "RowsQueryEvent"
	case WriteRowsEventTypeV2:
		return "WriteRowsEventV2"
	case UpdateRowsEventTypeV2:
		return "UpdateRowsEventV2"
	case DeleteRowsEventTypeV2:
		return "DeleteRowsEventV2"
	case GtidEventType:
		return "GtidEvent"
	case AnonymousGtidEventType:
		return "AnonymousGtidEvent"
	case PreviousGtidsEventType:
		return "PreviousGtidsEvent"
	case TransactionContextEventType:
		return "TransactionContextEvent"
	case ViewChangeEventType:
		return "ViewChangeEvent"
	case XaPrepareLogEventType:
		return "XaPrepareLogEvent"
	default:
		return "UnknownEvent"
	}
}
