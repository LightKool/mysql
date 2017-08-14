package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"io"
)

type Packet struct {
	data []byte
	pos  int
}

func NewPacket(data []byte) *Packet {
	return &Packet{data: data}
}

func (p *Packet) Raw() []byte {
	return p.data
}

// Len returns the total length of the buf of this packet.
func (p *Packet) Len() int {
	return len(p.data)
}

// EOF returns if the buffer of this packet has been consumed completely.
func (p *Packet) EOF() bool {
	return p.pos == len(p.data)
}

func (p *Packet) SliceRight(length int) (slice []byte) {
	if length > len(p.data) {
		return []byte{}
	}
	offset := len(p.data) - length
	p.data, slice = p.data[:offset], p.data[offset:]
	return
}

func (p *Packet) Skip(step int) {
	p.pos += step
}

func (p *Packet) Read(size int) (result []byte) {
	if size < 0 {
		result = p.data[p.pos:]
		p.pos = len(p.data)
	} else {
		result = p.data[p.pos : p.pos+size]
		p.pos += size
	}
	return
}

func (p *Packet) ReadUintBySize(size int) (u uint64) {
	switch {
	case size == 0:
		u = 0
	case size == 1:
		u = uint64(p.data[p.pos])
	case size == 2:
		u = uint64(binary.LittleEndian.Uint16(p.data[p.pos:]))
	case size >= 3 && size <= 4:
		var u32 uint32
		for i := 0; i < size; i++ {
			u32 |= uint32(p.data[p.pos+i]) << (uint(i) * 8)
		}
		u = uint64(u32)
	case size >= 5 && size <= 8:
		for i := 0; i < size; i++ {
			u |= uint64(p.data[p.pos+i]) << (uint(i) * 8)
		}
	default:
		panic("size must be between 0 and 8")
	}
	p.pos += size
	return
}

func (p *Packet) ReadUintBySizeBE(size int) (u uint64) {
	switch {
	case size == 0:
		u = 0
	case size == 1:
		u = uint64(p.data[p.pos])
	case size == 2:
		u = uint64(binary.BigEndian.Uint16(p.data[p.pos:]))
	case size >= 3 && size <= 4:
		var u32 uint32
		for i := 0; i < size; i++ {
			u32 |= uint32(p.data[p.pos+i]) << (uint(size-i-1) * 8)
		}
		u = uint64(u32)
	case size >= 5 && size <= 8:
		for i := 0; i < size; i++ {
			u |= uint64(p.data[p.pos+i]) << (uint(size-i-1) * 8)
		}
	default:
		panic("size must be between 0 and 8")
	}
	p.pos += size
	return
}

func (p *Packet) ReadPackedInteger() uint64 {
	num, _, n := readLengthEncodedInteger(p.data[p.pos:])
	p.pos += n
	return num
}

func (p *Packet) ReadPackedString() ([]byte, error) {
	data, _, n, err := readLengthEncodedString(p.data[p.pos:])
	if err != nil {
		return nil, err
	}
	p.pos += n
	return data, nil
}

// ConnWrapper wraps the unexported `mysqlConn` to export its functionalities.
type ConnWrapper struct {
	*mysqlConn
	drv driver.Driver
}

// NewConnWrapper create a new `mysql.ConnWrapper` instance.
func NewConnWrapper() *ConnWrapper {
	return &ConnWrapper{drv: &MySQLDriver{}}
}

// Connect to the MySQL server.
func (cw *ConnWrapper) Connect(dsn string) error {
	conn, err := cw.drv.Open(dsn)
	if err != nil {
		return err
	}
	cw.mysqlConn = conn.(*mysqlConn)
	return nil
}

// ReadOK reads and checks the OK packet returned from the MySQL server.
func (cw *ConnWrapper) ReadOK() error {
	_, err := cw.readResultOK()
	return err
}

// ReadPacket read returned data from the MySQL server.
func (cw *ConnWrapper) ReadPacket() ([]byte, error) {
	data, err := cw.readPacket()
	if err != nil {
		return nil, err
	}
	switch data[0] {
	case iEOF:
		return nil, io.EOF
	case iERR:
		return nil, cw.handleErrorPacket(data)
	default:
		buf := make([]byte, len(data)-1)
		copy(buf, data[1:])
		return buf, nil
	}
}

// WriteRegisterSlaveCommand send `RegisterSlave` command to the MySQL server.
func (cw *ConnWrapper) WriteRegisterSlaveCommand(serverID uint32, localhost, user, password string, port uint16) error {
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

	// replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	return cw.writeCommandPacketStr(comRegisterSlave, string(data))
}

// WriteBinlogDumpCommand sends the `BinlogDump` command to the MySQL server.
func (cw *ConnWrapper) WriteBinlogDumpCommand(serverID uint32, file string, position uint32) error {
	data := make([]byte, 4+2+4+len(file))
	pos := 0

	binary.LittleEndian.PutUint32(data[pos:], position)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], 0)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], serverID)
	pos += 4

	copy(data[pos:], file)

	return cw.writeCommandPacketStr(comBinlogDump, string(data))
}
