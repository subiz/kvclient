package kvclient

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/subiz/errors"
	"sync"
	"time"
)

const (
	tblKV    = "kv2"
	keyspace = "kvkvs"
)

var (
	readyLock = &sync.Mutex{}
	ready     bool
	session   *gocql.Session
)

func Init(seeds []string) {
	go func() {
		readyLock.Lock()

		// connect to cassandra cluster
		cluster := gocql.NewCluster("cas-0")
		cluster.Timeout = 10 * time.Second
		cluster.Keyspace = keyspace
		var err error
		for {
			if session, err = cluster.CreateSession(); err == nil {
				break
			}
			fmt.Println("cassandra", err, ". Retring after 5sec...")
			time.Sleep(5 * time.Second)
		}

		ready = true
		readyLock.Unlock()
	}()
}

func waitUntilReady() {
	if ready {
		return
	}
	readyLock.Lock()
	readyLock.Unlock()
}

// Get returns the value matched the provided key
// Note that this function dont return error when the value is not existed. Instead,
// it returns an empty value "" and a boolean `true` indicate that the value is empty
//
// scope is a required paramenter, used as a namespace to prevent collision between
// multiple services while using this lib concurrently.
// E.g: kvclient.Set("user", "324234", "onetwothree")
//      kvclient.Get("user", "324234") => "onetwothree"
func Get(scope, key string) (string, bool, error) {
	waitUntilReady()
	key = scope + "@" + key
	var val string
	err := session.Query(`SELECT v FROM `+tblKV+` WHERE k=?`, key).Scan(&val)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return "", false, nil
	}

	if err != nil {
		return "", false, errors.Wrap(err, 500, errors.E_database_error, "unable to read key %s", key)
	}

	return val, true, nil
}

// Set puts a new key-value pair to the database
// scope is a required paramenter, used as a namespace to prevent collision between
// multiple services while using this lib concurrently.
// E.g: kvclient.Set("user", "324234", "onetwothree")
// E.g: kvclient.Set("account", "324234", "onetwothree")
func Set(scope, key, value string) error {
	waitUntilReady()
	key = scope + "@" + key
	// ttl 60 days
	err := session.Query(`INSERT INTO `+tblKV+`(k,v) VALUES(?,?) USING TTL 5184000`, key, value).Exec()
	if err != nil {
		return errors.Wrap(err, 500, errors.E_database_error, "unable to read key %s", key)
	}

	return nil
}

// Del removes key from the database
// scope is a required paramenter, used as a namespace to prevent collision between
// multiple services while using this lib concurrently.
// E.g: kvclient.Del("user", "324234")
func Del(scope, key string) error {
	waitUntilReady()
	key = scope + "@" + key
	err := session.Query(`DELETE FROM `+tblKV+` WHERE k=?`, key).Exec()
	if err != nil {
		return errors.Wrap(err, 500, errors.E_database_error, "unable to read key %s", key)
	}
	return nil
}
