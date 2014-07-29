package cqlr

import (
	"crypto/rand"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"reflect"
	"speter.net/go/exp/math/dec/inf"
	"testing"
	"time"
)

func TestReflectionOnly(t *testing.T) {

	type Tweet struct {
		Timeline string
		Id       gocql.UUID
		Text     string
	}

	s := setup(t, "tweet")

	tweets := 5

	for i := 0; i < tweets; i++ {
		tw := Tweet{
			Timeline: "me",
			Id:       gocql.TimeUUID(),
			Text:     fmt.Sprintf("hello world %d", i),
		}

		if err := Bind(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`, tw).Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	q := s.Query(`SELECT text, id, timeline FROM tweet WHERE timeline = ?`, "me")
	b := BindQuery(q)

	count := 0
	var tw Tweet

	for b.Scan(&tw) {
		count++
		assert.Equal(t, "me", tw.Timeline)
	}

	err := b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, tweets, count)
}

func TestTagsOnly(t *testing.T) {

	type Reading struct {
		What    int       `cql:"id"`
		When    time.Time `cql:"timestamp"`
		HowMuch float32   `cql:"temperature"`
	}

	s := setup(t, "sensors")

	measurements := 11

	for i := 0; i < measurements; i++ {
		r := Reading{
			What:    i,
			When:    time.Now(),
			HowMuch: float32(1) / 3,
		}

		if err := Bind(`INSERT INTO sensors (id, timestamp, temperature) VALUES (?, ?, ?)`, r).Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	q := s.Query(`SELECT id, timestamp, temperature FROM sensors`)

	b := BindQuery(q)

	count := 0
	total := 0
	var r Reading

	for b.Scan(&r) {
		count++
		total += r.What
		assert.True(t, r.When.Before(time.Now()))
	}

	err := b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, measurements, count)
	assert.Equal(t, measurements*(measurements-1)/2, total) // http://en.wikipedia.org/wiki/Triangular_number
}

func TestLowLevelAPIOnly(t *testing.T) {

	type CDR struct {
		Imsi      string
		Timestamp time.Time
		Duration  int64
		Carrier   string
		Cost      *inf.Dec
	}

	strat := func(c gocql.ColumnInfo) (reflect.StructField, bool) {
		st := reflect.TypeOf((*CDR)(nil)).Elem()
		switch c.Name {
		case "imsi":
			return st.FieldByName("Imsi")
		case "timestamp":
			return st.FieldByName("Timestamp")
		case "duration":
			return st.FieldByName("Duration")
		case "carrier":
			return st.FieldByName("Carrier")
		case "charge":
			return st.FieldByName("Cost")
		default:
			return reflect.StructField{}, false
		}
	}

	s := setup(t, "calls")

	measurements := 43

	start := time.Now()

	for i := 0; i < measurements; i++ {

		cost := new(inf.Dec)
		cost.SetString(fmt.Sprintf("1.0%d", i))

		cdr := CDR{
			Imsi:      "240080852000132",
			Timestamp: start.Add(time.Duration(i) * time.Millisecond),
			Duration:  int64(i) + 60,
			Carrier:   "TMOB",
			Cost:      cost,
		}

		if err := Bind(`INSERT INTO calls (imsi, timestamp, duration, carrier, charge) VALUES (?, ?, ?, ?, ?)`, cdr).Use(strat).Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	q := s.Query(`SELECT imsi, timestamp, duration, carrier, charge FROM calls`)

	b := BindQuery(q).Use(strat)

	count := 0
	var r CDR

	for b.Scan(&r) {
		count++
		assert.Equal(t, "TMOB", r.Carrier)
	}

	err := b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, measurements, count)
}

func TestHighLevelAPIOnly(t *testing.T) {

	type Message struct {
		Identifier gocql.UUID
		Epoch      int64
		User       string
		Payload    []byte
	}

	strat := map[string]string{
		"id":   "Identifier",
		"unix": "Epoch",
		"usr":  "User",
		"msg":  "Payload",
	}

	s := setup(t, "queue")

	msgs := 163

	for i := 0; i < msgs; i++ {
		msg := make([]byte, 64)
		_, err := rand.Read(msg)
		if err != nil {
			t.Fatal(err)
		}

		m := Message{
			Identifier: gocql.TimeUUID(),
			Epoch:      time.Now().Unix(),
			User:       "deamon",
			Payload:    msg,
		}

		if err := Bind(`INSERT INTO queue (id, unix, usr, msg) VALUES (?, ?, ?, ?)`, m).Map(strat).Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	q := s.Query(`SELECT id, unix, usr, msg FROM queue`)

	b := BindQuery(q).Map(strat)

	count := 0
	var m Message

	for b.Scan(&m) {
		count++
		assert.Equal(t, "deamon", m.User)
	}

	err := b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, msgs, count)

}

func TestMixedBinding(t *testing.T) {

	type WaterLevel struct {
		Country       string // Bind by name
		When          int32  `cql:"year"` // Bind by tag
		Level         int64  // Bind using a map
		Precipitation int32  // Bind with a strategy
	}

	strat1 := map[string]string{
		"height": "Level",
	}

	strat2 := func(c gocql.ColumnInfo) (reflect.StructField, bool) {
		if c.Name == "rain" {
			st := reflect.TypeOf((*WaterLevel)(nil)).Elem()
			return st.FieldByName("Precipitation")
		} else {
			return reflect.StructField{}, false
		}
	}

	s := setup(t, "levels")

	entries := 79
	basePrecipitation := int32(100)
	baseLevel := int64(1000)
	startYear := int32(1850)

	for i := 0; i < entries; i++ {

		l := WaterLevel{
			Country:       "Antarctica",
			When:          1850 + int32(i),
			Level:         11*int64(i) + baseLevel,
			Precipitation: basePrecipitation + int32(i)*3,
		}

		if err := Bind(`INSERT INTO levels (country, year, height, rain) VALUES (?, ?, ?, ?)`, l).Map(strat1).Use(strat2).Exec(s); err != nil {
			t.Fatal(err)
		}

	}

	q := s.Query(`SELECT country, year, height, rain FROM levels`)

	b := BindQuery(q).Map(strat1)

	b.Use(strat2)

	count := 0
	var w WaterLevel

	for b.Scan(&w) {
		count++
		assert.Equal(t, "Antarctica", w.Country)
		assert.True(t, w.Level > (baseLevel-1))
		assert.True(t, w.When > (startYear-1))
		assert.True(t, w.Precipitation > (basePrecipitation-1))
	}

	err := b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, entries, count)
}

func TestIgnoreUnknownFields(t *testing.T) {

	type Simple struct {
		Id int
	}

	s := setup(t, "partial")

	if err := s.Query(`INSERT INTO partial (id, value) VALUES (?, ?)`, 11, "foo").Exec(); err != nil {
		t.Fatal(err)
	}

	q := s.Query(`SELECT id, value FROM partial`)

	b := BindQuery(q)

	var si Simple

	for b.Scan(&si) {
		assert.Equal(t, 11, si.Id)
	}

	err := b.Close()
	assert.Nil(t, err, "Could not close binding")
}

func TestStrictMapping(t *testing.T) {

	type Simple struct {
		Id int
	}

	s := setup(t, "partial")

	if err := s.Query(`INSERT INTO partial (id, value) VALUES (?, ?)`, 11, "foo").Exec(); err != nil {
		t.Fatal(err)
	}

	q := s.Query(`SELECT id, value FROM partial`)

	b := BindQuery(q).Strict()

	var si Simple

	for b.Scan(&si) {
		assert.Equal(t, 11, si.Id)
	}

	err := b.Close()
	assert.Equal(t, err, ErrMissingStrategy)
}

func TestIgnoreUnknownColumns(t *testing.T) {

	type Complex struct {
		Id        int
		Value     string
		Timestamp time.Time
	}

	s := setup(t, "partial")

	if err := s.Query(`INSERT INTO partial (id, value) VALUES (?, ?)`, 122, "bar").Exec(); err != nil {
		t.Fatal(err)
	}

	q := s.Query(`SELECT id, value FROM partial`)

	b := BindQuery(q)

	var c Complex

	for b.Scan(&c) {
		assert.Equal(t, 122, c.Id)
		assert.Equal(t, "bar", c.Value)
		assert.True(t, time.Time.IsZero(c.Timestamp))
	}

	err := b.Close()
	assert.Nil(t, err, "Could not close binding")
}

//TestNoCaseColumns is a test case to verify case insensitive columns are mapped properly
func TestNoCaseColumns(t *testing.T) {

	type Tweet struct {
		TimeLine string
		Id       gocql.UUID
		Text     string
	}

	s := setup(t, "tweet")
	defer s.Close()

	tw := Tweet{
		TimeLine: "me",
		Id:       gocql.TimeUUID(),
		Text:     fmt.Sprintf("hello world %d", 1),
	}

	if err := Bind(`INSERT INTO tweet (timeline, Id, Text) VALUES (?, ?, ?)`, tw).Exec(s); err != nil {
		t.Fatal(err)
	}

}

//TestCasedColumns is a test case to verify case sensitive columns are mapped properly
func TestCasedColumns(t *testing.T) {

	type Tweet struct {
		TimeLine string
		Id       gocql.UUID
		Text     string
	}

	s := setup(t, "tweetcase")
	defer s.Close()

	tw := Tweet{
		TimeLine: "me",
		Id:       gocql.TimeUUID(),
		Text:     fmt.Sprintf("hello world %d", 1),
	}

	if err := Bind(`INSERT INTO tweetcase ("timeLine", Id, Text) VALUES (?, ?, ?)`, tw).Exec(s); err != nil {
		t.Fatal(err)
	}

}

func setup(t *testing.T, table string) *gocql.Session {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "cqlr"
	s, err := cluster.CreateSession()

	assert.Nil(t, err, "Could not connect to keyspace")

	if err := s.Query(fmt.Sprintf("TRUNCATE %s", table)).Exec(); err != nil {
		t.Fatal(err)
	}

	return s
}
