cqlr
----

[![Build Status](https://travis-ci.org/relops/cqlr.png?branch=master)](https://travis-ci.org/relops/cqlr)

cqlr extends the [gocql][] runtime API and adds the ability to auto-bind a CQL iterator to a struct:

```go
type Tweet struct {
	Timeline string     `cql:"timeline"`
	Id       gocql.UUID `cql:"id"`
	Text     string     `cql:"text"`
}

iter := s.Query(`SELECT text, id, timeline FROM tweet WHERE timeline = ?`, "me").Iter()
b := cqlr.Bind(iter)

var t Tweet
for b.Scan(&t) {
	// Application specific code goes here
}
```

## Supported Binding Mechanisms

Right now, cqlr supports the following mechanisms to auto-bind iterators:

* Application supplied binding function
* Map of column name to struct field name
* By struct tags
* By field names

## Cassandra Support

Right now cqlr is tested against Cassandra 2.0.9.

## Motivation

gocql users are looking for ways to automatically bind query results to application defined structs, but this functionality is not available in the core library. In addition, it is possible that the core library does not want to support this feature, because it significantly increases the functional scope of that codebase. So the goal of cqlr is to see if this functionality can be layered on top of the core gocql API in a re-useable way.

## Design

cqlr should sit on top of the core gocql runtime and concern itself only with struct binding. The binding object is a stateful instance that wraps a gocql `Iter` and performs runtime introspection of the target struct. The binding is specifically stateful so that down the line, the first loop execution can perform expensive introspection and subsequent loop invocations can benefit from this cached runtime metadata. So in a sense, it is a bit like [cqlc][], except that the metadata processing is done on the first loop, rather than at compile time.

## Status

Right now this is an experiment to try to come up with a design that people think is useful and can be implemented sanely.

[gocql]: https://github.com/gocql/gocql
[cqlc]: https://github.com/relops/cqlc