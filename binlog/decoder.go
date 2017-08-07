package binlog

import (
	"github.com/LightKool/mysql-go"
)

type EventDecoder struct {
	format *FormatDescriptionEvent
	tables map[uint64]*TableMapEvent
}

func (dec *EventDecoder) Decode(packet *mysql.Packet) (Event, error) {
	header := &EventHeader{packet: newBinlogPacket(packet)}
	err := header.Decode(dec)
	if err != nil {
		return nil, err
	}

	var ev Event
	base := &baseEvent{header: header}
	if header.Type == FormatDescriptionEventType {
		dec.format = &FormatDescriptionEvent{baseEvent: base}
		ev = dec.format
	} else {
		switch header.Type {
		case RotateEventType:
			ev = &RotateEvent{baseEvent: base}
		case QueryEventType:
			ev = &QueryEvent{baseEvent: base}
		case XidEventType:
			ev = &XIDEvent{baseEvent: base}
		case RowsQueryEventType:
			ev = &RowsQueryEvent{baseEvent: base}
		case GtidEventType:
			ev = &GtidEvent{baseEvent: base}
		case TableMapEventType:
			ev = &TableMapEvent{baseEvent: base}
		case WriteRowsEventTypeV2:
			ev = &WriteRowsEvent{baseEvent: base}
		default:
			ev = &UnsupportedEvent{baseEvent: base}
		}
	}

	if err = ev.Decode(dec); err != nil {
		return nil, err
	}

	return ev, nil
}
