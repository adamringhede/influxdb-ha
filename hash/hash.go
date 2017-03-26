package hash

import (
	"hash/fnv"
	"strconv"
)

func String(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func Int(i int) uint32 {
	return String(strconv.Itoa(i))
}

func Float(f float64) uint32 {
	return String(strconv.FormatFloat(f, 'f', 10, 64))
}

func Bool(b bool) uint32 {
	return String(strconv.FormatBool(b))
}

func Any(d interface{}) uint32 {
	switch v := d.(type) {
	case int: return Int(v)
	case float64: return Float(v)
	case bool: return Bool(v)
	case string: return String(v)
	default:
		return 0
	}
}
