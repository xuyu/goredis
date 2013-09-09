package redis

type ReaderError struct {
	message string
}

func (err *ReaderError) Error() string {
	return err.message
}

type RedisError struct {
	message []byte
}

func (err *RedisError) Error() string {
	return string(err.message)
}

type ReplyTypeError struct {
	head []byte
}

func (err *ReplyTypeError) Error() string {
	return string(err.head)
}

type NilBulkError struct {
}

func (err *NilBulkError) Error() string {
	return "Nil Bulk Error"
}
