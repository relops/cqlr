schema:
	cqlsh -f test/keyspace.cql
	cqlsh -k cqlr -f test/schema.cql

test: schema
	go test -v ./...