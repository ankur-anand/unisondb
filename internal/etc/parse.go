package etc

import (
	"strconv"
	"strings"
)

func ParseSize(sizeStr string) int64 {
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))
	if strings.HasSuffix(sizeStr, "KB") {
		size, _ := strconv.ParseInt(strings.TrimSuffix(sizeStr, "KB"), 10, 64)
		return size * 1024
	} else if strings.HasSuffix(sizeStr, "MB") {
		size, _ := strconv.ParseInt(strings.TrimSuffix(sizeStr, "MB"), 10, 64)
		return size * 1024 * 1024
	} else if strings.HasSuffix(sizeStr, "GB") {
		size, _ := strconv.ParseInt(strings.TrimSuffix(sizeStr, "GB"), 10, 64)
		return size * 1024 * 1024 * 1024
	}
	// Default
	size, _ := strconv.ParseInt(sizeStr, 10, 64)
	return size
}
