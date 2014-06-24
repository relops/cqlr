package cqlr

import (
	"github.com/gocql/gocql"
	"reflect"
	"unicode"
)

type Binding struct {
	err  error
	iter *gocql.Iter
}

func Bind(iter *gocql.Iter) *Binding {
	return &Binding{iter: iter}
}

func (b *Binding) Close() error {
	return b.err
}

func (b *Binding) Scan(dest interface{}) bool {

	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Ptr || v.IsNil() {
		return false
	}

	cols := b.iter.Columns()
	values := make([]interface{}, len(cols))

	for i, col := range cols {
		f := reflect.Indirect(v).FieldByName(col.Name)

		if !f.IsValid() {
			f = reflect.Indirect(v).FieldByName(upcaseInitial(col.Name))
		}

		if !f.IsValid() {
			return false
		}

		values[i] = f.Addr().Interface()
	}

	return b.iter.Scan(values...)
}

func upcaseInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}
