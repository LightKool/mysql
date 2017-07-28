package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
)

const (
	fieldTypeTimestampV2 byte = iota + 0x11
	fieldTypeDateTimeV2
	fieldTypeTimeV2
)

type Packet struct {
	data []byte
	pos  int
}

func NewPacket(data []byte) *Packet {
	return &Packet{data: data, pos: 1}
}

func (p *Packet) Validate() error {
	switch p.data[0] {
	case iEOF:
		return io.ErrUnexpectedEOF
	case iERR:
		errno := p.ReadUint16()
		if p.data[p.pos] == 0x23 {
			p.pos = 9
		}
		return &MySQLError{errno, string(p.data[p.pos:])}
	default:
		return nil
	}
}

func (p *Packet) Raw() []byte {
	return p.data[1:]
}

func (p *Packet) Len() int {
	return len(p.data) - 1
}

func (p *Packet) TrimRight(length int) (*Packet, []byte) {
	if length > len(p.data) {
		return nil, []byte{}
	}
	return &Packet{data: p.data[:len(p.data)-length], pos: p.pos}, p.data[len(p.data)-length:]
}

func (p *Packet) Advance(step int) {
	p.pos += step
}

func (p *Packet) Read(size int) []byte {
	if size <= 0 {
		return []byte{}
	}
	result := p.data[p.pos : p.pos+size]
	p.pos += size
	return result
}

func (p *Packet) ReadByte() byte {
	result := p.data[p.pos]
	p.pos++
	return result
}

func (p *Packet) ReadRemaining() []byte {
	result := p.data[p.pos:]
	p.pos = len(p.data)
	return result
}

func (p *Packet) ReadUint16() uint16 {
	result := binary.LittleEndian.Uint16(p.data[p.pos:])
	p.pos += 2
	return result
}

func (p *Packet) ReadUint32() uint32 {
	result := binary.LittleEndian.Uint32(p.data[p.pos:])
	p.pos += 4
	return result
}

func (p *Packet) ReadUint64() uint64 {
	result := binary.LittleEndian.Uint64(p.data[p.pos:])
	p.pos += 8
	return result
}

func (p *Packet) ReadPackedInteger() uint64 {
	num, _, n := readLengthEncodedInteger(p.data[p.pos:])
	p.pos += n
	return num
}

func (p *Packet) ReadTableColumnMeta(columnTypes []byte) ([]uint16, error) {
	data, _, n, err := readLengthEncodedString(p.data[p.pos:])
	if err != nil {
		return nil, err
	}
	// decode table column metadata
	meta := make([]uint16, len(columnTypes))
	pos := 0
	for i, v := range columnTypes {
		switch v {
		case fieldTypeFloat, fieldTypeDouble, fieldTypeBLOB, fieldTypeJSON, fieldTypeGeometry:
			meta[i] = uint16(data[pos])
			pos++
		case fieldTypeBit, fieldTypeVarChar:
			meta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case fieldTypeString, fieldTypeVarString, fieldTypeNewDecimal:
			meta[i] = binary.BigEndian.Uint16(data[pos:])
			pos += 2
		case fieldTypeTimestampV2, fieldTypeDateTimeV2, fieldTypeTimeV2:
			meta[i] = uint16(data[pos])
			pos++
		default:
			meta[i] = 0
		}
	}
	p.pos += n
	return meta, nil
}

type MysqlConnWrapper struct {
	*mysqlConn
}

func NewMysqlConnWrapper(conn driver.Conn) (*MysqlConnWrapper, error) {
	mc, ok := conn.(*mysqlConn)
	if !ok {
		return nil, errors.New("unsupported sql driver")
	}
	return &MysqlConnWrapper{mc}, nil
}

func (wr *MysqlConnWrapper) ReadOK() (*Packet, error) {
	data, err := wr.readResultOK()
	return NewPacket(data), err
}

func (wr *MysqlConnWrapper) ReadPacket() (*Packet, error) {
	data, err := wr.readPacket()
	return NewPacket(data), err
}

func (wr *MysqlConnWrapper) WriteRegisterSlaveCommand(serverID uint32, localhost, user, password string, port uint16) error {
	data := make([]byte, 4+1+len(localhost)+1+len(user)+1+len(password)+2+4+4)
	pos := 0

	binary.LittleEndian.PutUint32(data[pos:], serverID)
	pos += 4

	data[pos] = byte(len(localhost))
	pos++
	n := copy(data[pos:], localhost)
	pos += n

	data[pos] = byte(len(user))
	pos++
	n = copy(data[pos:], user)
	pos += n

	data[pos] = byte(len(password))
	pos++
	n = copy(data[pos:], password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], port)
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	return wr.writeCommandPacketStr(comRegisterSlave, string(data))
}

func (wr *MysqlConnWrapper) WriteBinlogDumpCommand(serverID uint32, file string, position uint32) error {
	data := make([]byte, 4+2+4+len(file))
	pos := 0

	binary.LittleEndian.PutUint32(data[pos:], position)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], 0x00)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], serverID)
	pos += 4

	copy(data[pos:], file)

	return wr.writeCommandPacketStr(comBinlogDump, string(data))
}
