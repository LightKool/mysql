package binlog

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/LightKool/mysql-go"
	"github.com/juju/errors"
)

const (
	eventHeaderSize = 19
	dateTimeFormat  = "2006-01-02 15:04:05"
)

type EventHeader struct {
	Timestamp  uint32
	Type       EventType
	ServerID   uint32
	EventSize  uint32
	NextLogPos uint32
	Flags      uint16
}

func (h *EventHeader) Decode(packet *mysql.Packet) error {
	if packet.Len() < eventHeaderSize {
		return errors.Errorf("event header size %d too short, expect %d", packet.Len(), eventHeaderSize)
	}

	h.Timestamp = packet.ReadUint32()
	h.Type = EventType(packet.ReadByte())
	h.ServerID = packet.ReadUint32()
	h.EventSize = packet.ReadUint32()
	h.NextLogPos = packet.ReadUint32()
	h.Flags = packet.ReadUint16()

	if packet.Len() != int(h.EventSize) {
		return errors.Errorf("header event size: %d != actual event size: %d, maybe corrupted", h.EventSize, packet.Len())
	}
	return nil
}

func (h *EventHeader) DecodeEvent(packet *mysql.Packet) (Event, error) {
	return nil, nil
}

type Event interface {
	Header() *EventHeader
	Decode(*mysql.Packet) error
	Print(w io.Writer)
}

type baseEvent struct {
	header *EventHeader
}

func (e *baseEvent) Header() *EventHeader {
	return e.header
}

func (e *baseEvent) PrintHeader(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", e.header.Type)
	fmt.Fprintf(w, "Date: %s\n", time.Unix(int64(e.header.Timestamp), 0).Format(dateTimeFormat))
	fmt.Fprintf(w, "Log position: %d\n", e.header.NextLogPos)
	fmt.Fprintf(w, "Event size: %d\n", e.header.EventSize-eventHeaderSize)
}

type UnsupportedEvent struct {
	*baseEvent
	data []byte
}

func (e *UnsupportedEvent) Decode(packet *mysql.Packet) error {
	e.data = packet.Raw()
	return nil
}

func (e *UnsupportedEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "Data:\n%s\n", hex.Dump(e.data))
	fmt.Fprintln(w)
}

type RotateEvent struct {
	*baseEvent
	Position    uint64
	NextLogName []byte
}

func (e *RotateEvent) Decode(packet *mysql.Packet) error {
	e.Position = packet.ReadUint64()
	e.NextLogName = packet.ReadRemaining()
	return nil
}

func (e *RotateEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "Position: %d\n", e.Position)
	fmt.Fprintf(w, "Next log name: %s\n", e.NextLogName)
	fmt.Fprintln(w)
}

var (
	checksumEnabledMysqlVersion = parseMysqlVersion("5.6.1")
)

type FormatDescriptionEvent struct {
	*baseEvent
	BinlogVersion          uint16
	ServerVersion          []byte
	EventHeaderLength      uint8
	EventPostHeaderLengths []byte

	checksumAlg byte
}

func (e *FormatDescriptionEvent) Decode(packet *mysql.Packet) error {
	e.BinlogVersion = packet.ReadUint16()
	e.ServerVersion = bytes.Trim(packet.Read(50), "\x00")
	packet.Advance(4)
	e.EventHeaderLength = packet.ReadByte()
	if parseMysqlVersion(string(e.ServerVersion)).greaterOrEqual(checksumEnabledMysqlVersion) {
		var checksumPart []byte
		packet, checksumPart = packet.TrimRight(5)
		e.checksumAlg = checksumPart[0]
	}
	e.EventPostHeaderLengths = packet.ReadRemaining()
	return nil
}

func (e *FormatDescriptionEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "Binlog Version: %d\n", e.BinlogVersion)
	fmt.Fprintf(w, "Server version: %s\n", e.ServerVersion)
	e.printEventPostHeaderLengths(w)
	fmt.Fprintln(w)
}

func (e *FormatDescriptionEvent) printEventPostHeaderLengths(w io.Writer) {
	fmt.Fprintln(w, "Event post header lengths:")
	for i, v := range e.EventPostHeaderLengths {
		fmt.Fprintf(w, "\t%s: %d\n", EventType(i+1), v)
	}
}

func (e *FormatDescriptionEvent) checksumEnabled() bool {
	return e.checksumAlg == 1 // only support CRC checksum
}

type QueryEvent struct {
	*baseEvent
	ThreadID      uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	Database      []byte
	Query         []byte
}

func (e *QueryEvent) Decode(packet *mysql.Packet) error {
	e.ThreadID = packet.ReadUint32()
	e.ExecutionTime = packet.ReadUint32()
	databaseLen := packet.ReadByte()
	e.ErrorCode = packet.ReadUint16()
	statusVarsLen := packet.ReadUint16()
	e.StatusVars = packet.Read(int(statusVarsLen))
	e.Database = packet.Read(int(databaseLen))
	packet.Advance(1)
	e.Query = packet.ReadRemaining()
	return nil
}

func (e *QueryEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "Thread ID: %d\n", e.ThreadID)
	fmt.Fprintf(w, "Execution time: %d\n", e.ExecutionTime)
	fmt.Fprintf(w, "Error code: %d\n", e.ErrorCode)
	fmt.Fprintf(w, "Database: %s\n", e.Database)
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}

type XIDEvent struct {
	*baseEvent
	TransactionID uint64
}

func (e *XIDEvent) Decode(packet *mysql.Packet) error {
	e.TransactionID = packet.ReadUint64()
	return nil
}

func (e *XIDEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "TransactionID: %d\n", e.TransactionID)
	fmt.Fprintln(w)
}

type GtidEvent struct {
	*baseEvent
	CommitFlag uint8
	sid        []byte
	gno        uint64
}

func (e *GtidEvent) Decode(packet *mysql.Packet) error {
	e.CommitFlag = packet.ReadByte()
	e.sid = packet.Read(16)
	e.gno = packet.ReadUint64()
	return nil
}

func (e *GtidEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "Commit flag: %d\n", e.CommitFlag)
	fmt.Fprintf(w, "GTID: %s\n", e.GTID())
	fmt.Fprintln(w)
}

func (e *GtidEvent) GTID() string {
	buf := make([]byte, 36)

	hex.Encode(buf[0:8], e.sid[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], e.sid[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], e.sid[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], e.sid[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], e.sid[10:])

	return fmt.Sprintf("%s:%d", buf, e.gno)
}
