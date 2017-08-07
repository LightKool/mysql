package binlog

import (
	"regexp"
	"strconv"
	"strings"
)

type mysqlVersion struct {
	x, y, z int
}

func parseMysqlVersion(version string) *mysqlVersion {
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return &mysqlVersion{0, 0, 0}
	}
	x, _ := strconv.Atoi(parts[0])
	y, _ := strconv.Atoi(parts[1])
	re := regexp.MustCompile(`\d+`)
	z, _ := strconv.Atoi(re.FindString(parts[2]))
	return &mysqlVersion{x, y, z}
}

func (v *mysqlVersion) greaterOrEqual(other *mysqlVersion) bool {
	return (v.x<<16 | v.y<<8 | v.z) >= (other.x<<16 | other.y<<8 | other.z)
}

func isBitSet(bitmap []byte, i int) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}
