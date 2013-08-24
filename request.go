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

func build_request(args ...string) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(STAR); err != nil {
		return buf.Bytes(), err
	}
	if _, err := buf.WriteString(strconv.Itoa(len(args))); err != nil {
		return buf.Bytes(), err
	}
	if _, err := buf.Write(DELIM); err != nil {
		return buf.Bytes(), err
	}
	for _, arg := range args {
		if err := buf.WriteByte(DOLLAR); err != nil {
			return buf.Bytes(), err
		}
		if _, err := buf.WriteString(strconv.Itoa(len(arg))); err != nil {
			return buf.Bytes(), err
		}
		if _, err := buf.Write(DELIM); err != nil {
			return buf.Bytes(), err
		}
		if _, err := buf.WriteString(arg); err != nil {
			return buf.Bytes(), err
		}
		if _, err := buf.Write(DELIM); err != nil {
			return buf.Bytes(), err
		}
	}
	return buf.Bytes(), nil
}
