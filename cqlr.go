package cqlr

import (
	"errors"
	"github.com/gocql/gocql"
	"reflect"
	"unicode"
)

type Binding struct {
	err        error
	iter       *gocql.Iter
	isCompiled bool
	strict     bool
	strategy   map[string]reflect.Value
	fun        func(gocql.ColumnInfo) (reflect.StructField, bool)
	typeMap    map[string]string
}

func Bind(iter *gocql.Iter) *Binding {
	return &Binding{iter: iter, strategy: make(map[string]reflect.Value)}
}

func (b *Binding) Use(f func(gocql.ColumnInfo) (reflect.StructField, bool)) *Binding {
	b.fun = f
	return b
}

func (b *Binding) Map(m map[string]string) *Binding {
	b.typeMap = m
	return b
}

func (b *Binding) Strict() *Binding {
	b.strict = true
	return b
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
	if !b.isCompiled {
		if err := b.compile(v, cols); err != nil {
			b.err = err
			return false
		}
	}

	values := make([]interface{}, len(cols))

	for i, col := range cols {
		f, ok := b.strategy[col.Name]

		if ok {
			values[i] = f.Addr().Interface()
		}
	}

	return b.iter.Scan(values...)
}

func (b *Binding) compile(v reflect.Value, cols []gocql.ColumnInfo) error {

	indirect := reflect.Indirect(v)

	s := indirect.Type()

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		tag := f.Tag.Get("cql")
		if tag != "" {
			b.strategy[tag] = indirect.Field(i)
		}
	}

	if b.fun != nil {
		for _, col := range cols {
			staticField, ok := b.fun(col)
			if ok {
				b.strategy[col.Name] = indirect.FieldByIndex(staticField.Index)
			}
		}
	}

	if b.typeMap != nil && len(b.typeMap) > 0 {
		for _, col := range cols {
			fieldName, ok := b.typeMap[col.Name]
			if ok {
				f := indirect.FieldByName(fieldName)
				b.strategy[col.Name] = f
			}
		}
	}

	for _, col := range cols {

		_, ok := b.strategy[col.Name]
		if !ok {

			f := indirect.FieldByName(col.Name)
			if !f.IsValid() {
				f = indirect.FieldByName(upcaseInitial(col.Name))
			}

			if f.IsValid() {
				b.strategy[col.Name] = f
			}
		}
	}

	if b.strict {
		if len(b.strategy) != len(cols) {
			return ErrMissingStrategy
		}
	}

	b.isCompiled = true

	return nil
}

func upcaseInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}

var (
	ErrMissingStrategy = errors.New("insufficient column mapping")
)
