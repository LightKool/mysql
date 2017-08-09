package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/LightKool/mysql-go"
)

type binlogPacket struct {
	*mysql.Packet
}

func newBinlogPacket(data []byte) *binlogPacket {
	return &binlogPacket{mysql.NewPacket(data)}
}

func (p *binlogPacket) readByte() byte {
	return p.Read(1)[0]
}

func (p *binlogPacket) readUint16() uint16 {
	return uint16(p.ReadUintBySize(2))
}

func (p *binlogPacket) readUint24() uint32 {
	return uint32(p.ReadUintBySize(3))
}

func (p *binlogPacket) readUint32() uint32 {
	return uint32(p.ReadUintBySize(4))
}

func (p *binlogPacket) readUint64() uint64 {
	return p.ReadUintBySize(8)
}

func (p *binlogPacket) readTableColumnMeta(columnTypes []byte) ([]uint16, error) {
	data, err := p.ReadPackedString()
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
	return meta, nil
}

func (p *binlogPacket) readTableColumnValue(columnType byte, meta uint16, unsigned bool) (v interface{}, err error) {
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
		b := p.readByte()
		if unsigned {
			v = int64(b)
		} else {
			v = int64(int8(b))
		}
	case fieldTypeShort:
		if unsigned {
			v = int64(p.readUint16())
		} else {
			v = int64(int16(p.readUint16()))
		}
	case fieldTypeInt24:
		if unsigned {
			v = int64(p.readUint24())
		} else {
			u32 := p.readUint24()
			if u32 >= 0x800000 {
				u32 = ^u32 + 1 // 2's compliment
			}
			v = int64(u32)
		}
	case fieldTypeLong:
		if unsigned {
			v = int64(p.readUint32())
		} else {
			v = int64(int32(p.readUint32()))
		}
	case fieldTypeLongLong:
		if unsigned {
			u64 := p.readUint64()
			if u64 > math.MaxInt64 {
				v = fmt.Sprintln(u64)
			} else {
				v = int64(u64)
			}
		} else {
			v = int64(p.readUint64())
		}
	case fieldTypeFloat:
		v = math.Float32frombits(p.readUint32())
	case fieldTypeDouble:
		v = math.Float64frombits(p.readUint64())
	case fieldTypeNewDecimal:
		v, err = p.readNewDecimal(meta)
	case fieldTypeYear:
		v = 1900 + int(p.readByte())
	case fieldTypeDate:
		u32 := uint32(p.ReadUintBySize(3))
		v = fmt.Sprintf("%04d-%02d-%02d", u32>>9, (u32>>5)%16, u32%32)
	case fieldTypeTime:
		u32 := uint32(p.ReadUintBySize(3))
		v = fmt.Sprintf("%02d:%02d:%02d", u32/10000, (u32%10000)/100, u32%100)
	case fieldTypeTimeV2:
		v = p.readTimeV2(meta)
	case fieldTypeDateTime:
		// a number like YYYYMMDDhhmmss
		u64 := p.readUint64()
		d := u64 / 1000000
		t := u64 % 1000000
		v = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", d/10000, (d%10000)/100, d%100, t/10000, (t%10000)/100, t%100)
	case fieldTypeDateTimeV2:
		v = p.readDateTimeV2(meta)
	case fieldTypeTimestamp:
		v = time.Unix(int64(p.readUint32()), 0).UnixNano()
	case fieldTypeTimestampV2:
		sec := int64(p.ReadUintBySizeBE(4))
		msec := p.readMicroSeconds(int(meta), false)
		v = time.Unix(sec, msec*1000).UnixNano()
	case fieldTypeVarChar, fieldTypeVarString:
		length = int(meta)
		fallthrough
	case fieldTypeString:
		if length < 256 {
			length = int(p.readByte())
		} else {
			length = int(p.readUint16())
		}
		v = string(p.Read(length))
	case fieldTypeEnum:
		if length == 1 || length == 2 {
			v = int64(p.ReadUintBySize(length))
		} else {
			err = fmt.Errorf("Unknown ENUM pack legnth: %d", length)
		}
	case fieldTypeSet:
		if length >= 0 && length <= 8 {
			v = int64(p.ReadUintBySizeBE(length))
		} else {
			err = fmt.Errorf("Unknown SET pack length: %d", length)
		}
	case fieldTypeBit:
		nbits := (meta>>8)*8 + meta&0xFF
		length = (int(nbits) + 7) / 8
		if length >= 0 && length <= 8 {
			v = int64(p.ReadUintBySizeBE(length))
		} else {
			err = fmt.Errorf("Unknown BIT pack length: %d", length)
		}
	case fieldTypeBLOB, fieldTypeGeometry: // MySQL saves Geometry as Blob in binlog
		length = int(meta)
		blobLen := p.ReadUintBySize(length)
		v = p.Read(int(blobLen))
	case fieldTypeJSON:
		// TODO
	}
	return
}

var digitsPerInteger = 9
var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

// Refer to https://github.com/mysql/mysql-server/blob/5.6/strings/decimal.c (line 1341: decimal2bin())
func (p *binlogPacket) readNewDecimal(meta uint16) (float64, error) {
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

// readMicroSeconds reads fractional part of MySQL timestamp/datetime/time fields
func (p *binlogPacket) readMicroSeconds(dec int, negative bool) int64 {
	// dec is in the range(0,6)
	msecLen := (dec + 1) / 2
	var msec int64
	msec = int64(p.ReadUintBySizeBE(msecLen))
	if msec != 0 {
		if negative {
			msec -= int64(math.Pow(0x100, float64(msecLen)))
		}
		msec *= int64(math.Pow(100, float64(3-msecLen)))
	}
	return msec
}

func (p *binlogPacket) readDateTimeV2(meta uint16) (v string) {
	/*
	    1 bit  sign            (1 = positive, 0 = negative ignored) the negative value of a datetime doesn't make much sense
	   17 bits year*13+month   (year 0-9999, month 0-12)
	    5 bits day             (0-31)
	    5 bits hour            (0-23)
	    6 bits minute          (0-59)
	    6 bits second          (0-59)
	   24 bits microseconds    (0-999999)

	   Total: 64 bits = 8 bytes

	   SYYYYYYY.YYYYYYYY.YYdddddh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
	*/
	datetime := p.ReadUintBySizeBE(5)
	dec := int(meta)
	msec := p.readMicroSeconds(dec, false)

	yearmonth := datetime >> (40 - 1 - 17) & (1<<17 - 1)
	year := yearmonth / 13
	month := yearmonth % 13
	day := datetime >> (40 - 18 - 5) & (1<<5 - 1)
	hour := datetime >> (40 - 23 - 5) & (1<<5 - 1)
	minute := datetime >> (40 - 28 - 6) & (1<<6 - 1)
	sec := datetime >> (40 - 34 - 6) & (1<<6 - 1)

	v = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, sec, msec)
	v = v[0 : len(v)-6+dec]
	return
}

func (p *binlogPacket) readTimeV2(meta uint16) (v string) {
	/*
	   1 bit sign (1 = positive, 0 = negative)
	   1 bit unused (reserved for future extensions)
	   10 bits hour (0-838)
	   6 bits minute (0-59)
	   6 bits second (0-59)

	   (3 bytes in total)

	   + fractional-seconds storage (size depends on meta)
	*/
	time := int64(p.ReadUintBySizeBE(3)) - 0x800000
	negative := time < 0
	dec := int(meta)
	msec := p.readMicroSeconds(dec, negative)

	var sign string
	if negative {
		if msec != 0 {
			time++
		}
		time = time<<24 + msec
		time = -time
		msec = time % (1 << 24)
		time = time >> 24
		sign = "-"
	}

	hour := time >> (24 - 2 - 10) & (1<<10 - 1)
	minute := time >> (24 - 12 - 6) & (1<<6 - 1)
	sec := time >> (24 - 18 - 6) & (1<<6 - 1)

	v = fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, sec, msec)
	v = v[0 : len(v)-6+dec]
	return
}
