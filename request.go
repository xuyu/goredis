package redis

import (
	"strconv"
)

const (
	CR     byte = 13
	LF     byte = 10
	DOLLAR byte = 36
	COLON  byte = 58
	MINUS  byte = 45
	PLUS   byte = 43
	STAR   byte = 42
)

var (
	DELIM = []byte{CR, LF}
)

func FormatSize(head byte, size int) []byte {
	line := []byte{head}
	line = strconv.AppendInt(line, int64(size), 10)
	return append(line, DELIM...)
}

func (r *Redis) BuildRequest(args [][]byte) error {
	if _, err := r.Conn.Write(FormatSize(STAR, len(args))); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := r.Conn.Write(FormatSize(DOLLAR, len(arg))); err != nil {
			return err
		}
		if _, err := r.Conn.Write(arg); err != nil {
			return err
		}
		if _, err := r.Conn.Write(DELIM); err != nil {
			return err
		}
	}
	return nil
}
