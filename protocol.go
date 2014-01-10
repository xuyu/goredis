package goredis

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
)

func packArgs(items ...interface{}) (args []interface{}) {
	for _, item := range items {
		v := reflect.ValueOf(item)
		switch v.Kind() {
		case reflect.Slice:
			if v.IsNil() {
				continue
			}
			if v.Type().Elem().Kind() == reflect.Uint8 {
				args = append(args, string(v.Bytes()))
			} else {
				for i := 0; i < v.Len(); i++ {
					args = append(args, v.Index(i).Interface())
				}
			}
		case reflect.Map:
			if v.IsNil() {
				continue
			}
			for _, key := range v.MapKeys() {
				value := v.MapIndex(key)
				args = append(args, key.Interface(), value.Interface())
			}
		case reflect.String:
			if v.String() != "" {
				args = append(args, v.Interface())
			}
		default:
			args = append(args, v.Interface())
		}
	}
	return args
}

func packCommand(args ...interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if _, err := fmt.Fprintf(buf, "*%d\r\n", len(args)); err != nil {
		return nil, err
	}
	for _, arg := range args {
		var s string
		switch v := arg.(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		case int:
			s = strconv.Itoa(v)
		case int64:
			s = strconv.FormatInt(v, 10)
		case uint64:
			s = strconv.FormatUint(v, 10)
		case float64:
			s = strconv.FormatFloat(v, 'g', -1, 64)
		default:
			s = fmt.Sprint(arg)
		}
		if _, err := fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(s), s); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
