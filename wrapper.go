package mysql

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
)

// extra field types included in the binlog
const (
	fieldTypeTimestampV2 byte = iota + 0x11
	fieldTypeDateTimeV2
	fieldTypeTimeV2
)

type Packet struct {
	data []byte
	pos  int
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

func (p *Packet) Read(size int) []byte {
	if size <= 0 {
		return []byte{}
	}
	result := p.data[p.pos : p.pos+size]
	p.pos += size
	return result
}

func (p *Packet) ReadRemaining() []byte {
	result := p.data[p.pos:]
	p.pos = len(p.data)
	return result
}

func (p *Packet) ReadUintBySize(size int) (u uint64) {
	switch {
	case size == 0:
		u = 0
	case size == 1:
		u = uint64(p.data[p.pos])
	case size == 2:
		u = uint64(binary.LittleEndian.Uint16(p.data[p.pos:]))
	case size == 3 || size == 4:
		var u32 uint32
		for i := 0; i < size; i++ {
			u32 |= uint32(p.data[p.pos+i]) << uint(i) * 8
		}
		u = uint64(u32)
	case size >= 5 && size <= 8:
		for i := 0; i < size; i++ {
			u |= uint64(p.data[p.pos+i]) << uint(i) * 8
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
	case size == 3 || size == 4:
		var u32 uint32
		for i := size - 1; i >= 0; i++ {
			u32 |= uint32(p.data[p.pos+i]) << uint(i) * 8
		}
		u = uint64(u32)
	case size >= 5 && size <= 8:
		for i := size - 1; i >= 0; i++ {
			u |= uint64(p.data[p.pos+i]) << uint(i) * 8
		}
	default:
		panic("size must be between 0 and 8")
	}
	p.pos += size
	return
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
		case fieldTypeBit, fieldTypeVarChar, fieldTypeVarString:
			// - fieldTypeBit: {length of the field}/8, {length of the field} % 8
			// - fieldTypeVarChar | fieldTypeVarChar: {length of the field}(2 bytes)
			meta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case fieldTypeString, fieldTypeNewDecimal:
			// - fieldTypeString: {real type}, {pack of field length}
			// - fieldTypeNewDecimal: {precision}, {scale}
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

func (p *Packet) ReadTableColumnValue(columnType byte, meta uint16, unsigned bool) (v interface{}, err error) {
	var length int
	if columnType == fieldTypeString {
		if meta >= 256 {
			realType := byte(meta >> 8)
			if realType&0x30 != 0x30 {
				length = int(uint16(meta&0xFF) | uint16((realType&0x30)^0x30)<<4)
				columnType = realType | 0x30
			} else {
				length = int(meta & 0xFF)
				columnType = realType
			}
		} else {
			length = int(meta)
		}
	}

	switch columnType {
	case fieldTypeTiny:
		b := p.Read(1)[0]
		if unsigned {
			v = int64(b)
		} else {
			v = int64(int8(b))
		}
	case fieldTypeShort:
		if unsigned {
			v = int64(p.ReadUint16())
		} else {
			v = int64(int16(p.ReadUint16()))
		}
	case fieldTypeInt24:
		b := p.Read(3)
		if unsigned {
			v = int64(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16)
		} else {
			u32 := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
			if u32 > 0x800000 {
				u32 -= 0x1000000
			}
			v = int64(u32)
		}
	case fieldTypeLong:
		if unsigned {
			v = int64(p.ReadUint32())
		} else {
			v = int64(int32(p.ReadUint32()))
		}
	case fieldTypeLongLong:
		if unsigned {
			u64 := p.ReadUint64()
			if u64 > math.MaxInt64 {
				v = uint64ToString(u64)
			} else {
				v = int64(u64)
			}
		} else {
			v = int64(p.ReadUint64())
		}
	case fieldTypeFloat:
		v = math.Float32frombits(p.ReadUint32())
	case fieldTypeDouble:
		v = math.Float64frombits(p.ReadUint64())
	case fieldTypeNewDecimal:
		v, err = p.readNewDecimal(meta)
	case fieldTypeYear:
		v = 1900 + int(p.Read(1)[0])
	case fieldTypeDate:
		u32 := binary.LittleEndian.Uint32(append(p.Read(3), 0x00))
		v = fmt.Sprintf("%04d-%02d-%02d", u32>>9, (u32>>5)%16, u32%32)
	case fieldTypeTime:
		u32 := binary.LittleEndian.Uint32(append(p.Read(3), 0x00))
		if u32 == 0 {
			v = "00:00:00"
		} else {
			var sign string
			if u32 < 0 {
				sign = "-"
			}
			v = fmt.Sprintf("%s%02d:%02d:%02d", sign, u32/10000, (u32%10000)/100, u32%100)
		}
	case fieldTypeTimeV2:
		// v = p.readTimeV2(meta)
	case fieldTypeDateTime:
		// a number like YYYYMMDDhhmmss
		u64 := p.ReadUint64()
		d := u64 / 1000000
		t := u64 % 1000000
		v = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", d/10000, (d%10000)/100, d%100, t/10000, (t%10000)/100, t%100)
	case fieldTypeDateTimeV2:
		v = p.readDateTimeV2(meta)
	case fieldTypeTimestamp:
		u32 := p.ReadUint32()
		v = time.Unix(int64(u32), 0).UnixNano()
	case fieldTypeTimestampV2:
		sec := int64(binary.BigEndian.Uint32(p.Read(4)))
		msec := p.readFractionalSeconds(int(meta))
		if sec == 0 {
			v = sec
		} else {
			v = time.Unix(sec, msec*1000).UnixNano()
		}
	case fieldTypeVarChar, fieldTypeVarString:
		length = int(meta)
		fallthrough
	case fieldTypeString:
		if length < 256 {
			length = int(p.Read(1)[0])
		} else {
			length = int(p.ReadUint16())
		}
		v = string(p.Read(length))
	case fieldTypeEnum:
		if length == 1 || length == 2 {
			v = int64(binary.LittleEndian.Uint16(append(p.Read(length), make([]byte, 2-length)...)))
		} else {
			err = fmt.Errorf("Unknown ENUM pack legnth: %d", length)
		}
	case fieldTypeSet:
		if length == 0 {
			v = int64(0)
		} else if length > 0 && length <= 8 {
			v = int64(binary.LittleEndian.Uint64(append(p.Read(length), make([]byte, 8-length)...)))
		} else {
			err = fmt.Errorf("Unknown SET pack length: %d", length)
		}
	case fieldTypeBLOB, fieldTypeGeometry: // MySQL saves Geometry as Blob in binlog
		length = int(meta)
		blobLen := binary.LittleEndian.Uint32(append(p.Read(length), make([]byte, 4-length)...))
		v = p.Read(int(blobLen))
	case fieldTypeJSON:
		// TODO
	}
	return
}

var digitsPerInteger = 9
var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

// Refer to https://github.com/mysql/mysql-server/blob/5.6/strings/decimal.c (line 1341: decimal2bin())
func (p *Packet) readNewDecimal(meta uint16) (float64, error) {
	precision, scale := int(meta>>8), int(meta&0xFF)
	integral := precision - scale // digits number to the left of the decimal point
	intg, frac := integral/digitsPerInteger, scale/digitsPerInteger
	intgx, fracx := integral%digitsPerInteger, scale%digitsPerInteger
	size := compressedBytes[intgx] + intg*4 + frac*4 + compressedBytes[fracx]
	data := p.Read(size)

	var buf bytes.Buffer
	negative := data[0]&0x80 == 0
	if negative {
		for i := range data {
			data[i] ^= 0xFF // if negative, convert to positive
		}
		buf.WriteString("-")
	}
	data[0] ^= 0x80 // remove the sign bit

	var length, pos int
	// compressed integer part
	length = compressedBytes[intgx]
	buf.WriteString(fmt.Sprintf("%d", binary.BigEndian.Uint32(append(make([]byte, 4-length), data[pos:pos+length]...))))
	pos += length
	// uncompressed integer part
	for i := 0; i < intg; i++ {
		buf.WriteString(fmt.Sprintf("%09d", binary.BigEndian.Uint32(data[pos:])))
		pos += 4
	}
	// decimal point
	buf.WriteString(".")
	// uncompressed fractional part
	for i := 0; i < frac; i++ {
		buf.WriteString(fmt.Sprintf("%09d", binary.BigEndian.Uint32(data[pos:])))
		pos += 4
	}
	// compressed fractional part
	length = compressedBytes[fracx]
	buf.WriteString(fmt.Sprintf("%0*d", fracx, binary.BigEndian.Uint32(append(make([]byte, 4-length), data[pos:pos+length]...))))

	return strconv.ParseFloat(buf.String(), 64)
}

func (p *Packet) readFractionalSeconds(dec int) int64 {
	// dec is in the range(1,6)
	fracLen := (dec + 1) / 2
	var frac int64
	if fracLen > 0 {
		// padding to 4 bytes
		data := append(make([]byte, 4-fracLen), p.Read(fracLen)...)
		frac = int64(binary.BigEndian.Uint32(data))
		frac *= int64(math.Pow(100, float64(3-fracLen)))
	}
	return frac
}

func (p *Packet) readTimeV2(meta uint16) (v string) {
	/*
	   1 bit sign (1 = positive, 0 = negative)
	   1 bit unused (reserved for future extensions)
	   10 bits hour (0-838)
	   6 bits minute (0-59)
	   6 bits second (0-59)

	   (3 bytes in total)

	   + fractional-seconds storage (size depends on meta)
	*/
	data := p.Read(3)
	negative := data[0]&0x80 == 0
	time := binary.BigEndian.Uint32(append([]byte{0x00}, data...))
	if negative {
		time ^= 0xFFFFFF
	}

	hour := time >> (24 - 2 - 10) & (1<<10 - 1)
	minute := time >> (24 - 12 - 6) & (1<<6 - 1)
	sec := time >> (24 - 18 - 6) & (1<<6 - 1)

	v = fmt.Sprintf("%02d:%02d:%02d", hour, minute, sec)
	return
}

func (p *Packet) readDateTimeV2(meta uint16) (v string) {
	/*
	    1 bit  sign            (1 = positive, 0 = negative) could be ignored
	   17 bits year*13+month   (year 0-9999, month 0-12)
	    5 bits day             (0-31)
	    5 bits hour            (0-23)
	    6 bits minute          (0-59)
	    6 bits second          (0-59)
	   24 bits microseconds    (0-999999)

	   Total: 64 bits = 8 bytes

	   SYYYYYYY.YYYYYYYY.YYdddddh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
	*/
	data := p.Read(5)
	datetime := binary.BigEndian.Uint64(append(make([]byte, 3), data...))
	yearmonth := datetime >> (40 - 1 - 17) & (1<<17 - 1)
	year := yearmonth / 13
	month := yearmonth % 13
	day := datetime >> (40 - 18 - 5) & (1<<5 - 1)
	hour := datetime >> (40 - 23 - 5) & (1<<5 - 1)
	minute := datetime >> (40 - 28 - 6) & (1<<6 - 1)
	sec := datetime >> (40 - 34 - 6) & (1<<6 - 1)

	dec := int(meta)
	frac := p.readFractionalSeconds(dec)
	if frac == 0 {
		v = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, sec)
	} else {
		v = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, sec, frac)
		v = v[0 : len(v)-dec]
	}
	return
}

// ConnWrapper wraps the unexported `mysqlConn` to export its functionalities.
type ConnWrapper struct {
	*mysqlConn
	drv driver.Driver
}

// NewMysqlConnWrapper create a new `mysql.ConnWrapper` instance.
// Takes the mysql Driver as the sole argument.
func NewMysqlConnWrapper(drv driver.Driver) (*ConnWrapper, error) {
	drv, ok := drv.(*MySQLDriver)
	if !ok {
		return nil, errors.New("unsupported sql driver")
	}
	return &ConnWrapper{drv: drv}, nil
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
func (cw *ConnWrapper) ReadPacket() (*Packet, error) {
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
		return &Packet{data: buf}, nil
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
