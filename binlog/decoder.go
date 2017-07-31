package binlog

import (
	"github.com/LightKool/mysql-go"
)

type EventDecoder struct {
	format *FormatDescriptionEvent
}

func (dec *EventDecoder) Decode(packet *mysql.Packet) (Event, error) {
	header, err := dec.decodeHeader(packet)
	if err != nil {
		return nil, err
	}

	var ev Event
	base := &baseEvent{header: header}

	if header.Type == formatDescriptionEvent {
		dec.format = &FormatDescriptionEvent{baseEvent: base}
		ev = dec.format
	} else {
		if dec.format != nil && dec.format.checksumEnabled() {
			packet, _ = packet.TrimRight(4)
		}
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
		default:
			ev = &UnsupportedEvent{baseEvent: base}
		}
	}

	if err = ev.Decode(packet); err != nil {
		return nil, err
	}

	return ev, nil
}

func (dec *EventDecoder) decodeHeader(packet *mysql.Packet) (*EventHeader, error) {
	var header EventHeader
	err := header.Decode(packet)
	if err != nil {
		return nil, err
	}
	return &header, nil
}
