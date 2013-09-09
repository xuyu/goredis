package redis

import (
	"bytes"
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

func BuildRequest(args [][]byte) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if _, err := buf.Write(FormatSize(STAR, len(args))); err != nil {
		return buf.Bytes(), err
	}
	for _, arg := range args {
		if _, err := buf.Write(FormatSize(DOLLAR, len(arg))); err != nil {
			return buf.Bytes(), err
		}
		if _, err := buf.Write(arg); err != nil {
			return buf.Bytes(), err
		}
		if _, err := buf.Write(DELIM); err != nil {
			return buf.Bytes(), err
		}
	}
	return buf.Bytes(), nil
}
