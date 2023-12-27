package parse

import (
	"fmt"
	"math"
)

func humanBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%v B", b)
	}
	bf := float64(b)
	for _, unit := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		bf /= 1024.0
		if math.Abs(bf) < float64(1024) {
			return fmt.Sprintf("%.3f %s", bf, unit)
		}
	}
	return fmt.Sprintf("%v B", b)
}
