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
	switch header.Type {
	case FormatDescriptionEventType:
		ev = &FormatDescriptionEvent{baseEvent: be}
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
	case WriteRowsEventType, UpdateRowsEventType, DeleteRowsEventType:
		ev = &RowsEvent{baseEvent: be}
	default:
		ev = &UnsupportedEvent{baseEvent: be}
	}

	if err = ev.Decode(dec); err != nil {
		return nil, err
	}

	if pd, ok := ev.(postDecoder); ok {
		err = pd.postDecode(dec)
		if err != nil {
			return nil, err
		}
	}

	return ev, nil
}

type postDecoder interface {
	postDecode(*EventDecoder) error
}
