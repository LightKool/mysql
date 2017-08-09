package binlog

type EventDecoder struct {
	format *FormatDescriptionEvent
	tables map[uint64]*TableMapEvent
}

func (dec *EventDecoder) decode(data []byte) (Event, error) {
	header := &EventHeader{packet: newBinlogPacket(data)}
	err := header.Decode(dec)
	if err != nil {
		return nil, err
	}

	var ev Event
	be := &baseEvent{header: header}
	if header.Type == FormatDescriptionEventType {
		dec.format = &FormatDescriptionEvent{baseEvent: be}
		ev = dec.format
	} else {
		switch header.Type {
		case RotateEventType:
			ev = &RotateEvent{baseEvent: be}
		case QueryEventType:
			ev = &QueryEvent{baseEvent: be}
		case XidEventType:
			ev = &XIDEvent{baseEvent: be}
		case RowsQueryEventType:
			ev = &RowsQueryEvent{baseEvent: be}
		case GtidEventType:
			ev = &GtidEvent{baseEvent: be}
		case TableMapEventType:
			ev = &TableMapEvent{baseEvent: be}
		case WriteRowsEventType:
			ev = &WriteRowsEvent{baseEvent: be}
		case UpdateRowsEventType:
			ev = &UpdateRowsEvent{baseEvent: be}
		default:
			ev = &UnsupportedEvent{baseEvent: be}
		}
	}

	if err = ev.Decode(dec); err != nil {
		return nil, err
	}

	return ev, nil
}
