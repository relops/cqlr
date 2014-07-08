package cqlr

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Tweet struct {
	Timeline string
	Id       gocql.UUID
	Text     string
}

type TaggedTweet struct {
	Timeline string     `cql:"timeline"`
	Id       gocql.UUID `cql:"id"`
	Text     string     `cql:"text"`
}

func TestTweetBinding(t *testing.T) {

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "cqlr"
	s, err := cluster.CreateSession()
	defer s.Close()

	assert.Nil(t, err, "Could not connect to keyspace")

	if err := s.Query("TRUNCATE tweet").Exec(); err != nil {
		t.Fatal(err)
	}

	tweets := 5

	for i := 0; i < tweets; i++ {
		if err := s.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
			"me", gocql.TimeUUID(), fmt.Sprintf("hello world %d", i)).Exec(); err != nil {
			t.Fatal(err)
		}
	}

	var tw Tweet

	// Bind by reflection

	iter := s.Query(`SELECT text, id, timeline FROM tweet WHERE timeline = ?`, "me").Iter()

	b := Bind(iter)

	count := 0
	for b.Scan(&tw) {
		count++
		assert.Equal(t, "me", tw.Timeline)
	}

	err = b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, tweets, count)

	// Bind explicitly

	iter = s.Query(`SELECT text, id, timeline FROM tweet WHERE timeline = ?`, "me").Iter()

	b = BindFunc(iter, func(s string) string {
		switch s {
		case "text":
			return "Text"
		case "id":
			return "Id"
		case "timeline":
			return "Timeline"
		default:
			return ""
		}
	})

	count = 0
	for b.Scan(&tw) {
		count++
		assert.Equal(t, "me", tw.Timeline)
	}

	err = b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, tweets, count)

	// Bind by tag

	var ttw TaggedTweet

	iter = s.Query(`SELECT text, id, timeline FROM tweet WHERE timeline = ?`, "me").Iter()

	b = BindTag(iter)

	count = 0
	for b.Scan(&ttw) {
		count++
		assert.Equal(t, "me", ttw.Timeline)
	}

	err = b.Close()
	assert.Nil(t, err, "Could not close binding")
	assert.Equal(t, tweets, count)
}
