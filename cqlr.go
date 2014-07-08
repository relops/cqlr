package cqlr

import (
	"github.com/gocql/gocql"
	"reflect"
	"unicode"
)

type Binding struct {
	err            error
	iter           *gocql.Iter
	preferTags     bool
	preferExplicit bool
	preferMap      bool
	fun            func(string) (reflect.StructField, bool)
	typeMap        map[string]string
}

func Bind(iter *gocql.Iter) *Binding {
	return &Binding{iter: iter}
}

func BindTag(iter *gocql.Iter) *Binding {
	return &Binding{iter: iter, preferTags: true}
}

func BindFunc(iter *gocql.Iter, f func(string) (reflect.StructField, bool)) *Binding {
	return &Binding{iter: iter, fun: f, preferExplicit: true}
}

func BindMap(iter *gocql.Iter, m map[string]string) *Binding {
	return &Binding{iter: iter, typeMap: m, preferMap: true}
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
	indirect := reflect.Indirect(v)

	var mapper func(c gocql.ColumnInfo) (reflect.Value, bool)

	// Right now, this is all experimental to try to tease out the right API

	if b.preferTags {

		mapper = func(col gocql.ColumnInfo) (reflect.Value, bool) {
			mapping := make(map[string]reflect.Value)

			s := indirect.Type()

			for i := 0; i < s.NumField(); i++ {
				f := s.Field(i)
				tag := f.Tag.Get("cql")
				mapping[tag] = indirect.Field(i)
			}

			f, ok := mapping[col.Name]
			return f, ok
		}

	} else if b.preferExplicit {

		mapper = func(col gocql.ColumnInfo) (reflect.Value, bool) {
			staticField, ok := b.fun(col.Name)
			if ok {
				f := indirect.FieldByIndex(staticField.Index)
				return f, true
			}
			return reflect.Value{}, false
		}

	} else if b.preferMap {

		mapper = func(col gocql.ColumnInfo) (reflect.Value, bool) {

			fieldName, ok := b.typeMap[col.Name]
			if ok {
				f := indirect.FieldByName(fieldName)
				return f, true
			}
			return reflect.Value{}, false
		}

	} else {

		mapper = func(col gocql.ColumnInfo) (reflect.Value, bool) {
			f := indirect.FieldByName(col.Name)

			if !f.IsValid() {
				f = indirect.FieldByName(upcaseInitial(col.Name))
			}

			return f, f.IsValid()
		}
	}

	for i, col := range cols {
		f, ok := mapper(col)
		if ok {
			values[i] = f.Addr().Interface()
		}
	}

	return b.iter.Scan(values...)
}

func upcaseInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}
