package mapDB

import (
	"testing"
)

func TestMapDB(t *testing.T) {
	var (
		m   *MapDB
		err error

		entryCnt int
	)

	if m, err = New("db", "testing"); err != nil {
		t.Fatal(err)
	}

	m.Put("greeting", "Hello")
	m.Put("results", "none")
	m.Put("name", "world")
	m.Delete("results")
	m.Put("name", "John Doe")

	tm := map[string]string{
		"greeting": "Hello",
		"name":     "John Doe",
	}

	m.ForEach(func(key, value string) (end bool) {
		if tm[key] != value {
			t.Fatalf("invalid value, expected %s and received %s", tm[key], value)
		}

		entryCnt++
		return
	})

	if entryCnt != 2 {
		t.Fatalf("invalid entry count, expected %d and received %d", 2, entryCnt)
	}

	if err = m.Close(); err != nil {
		t.Fatal(err)
	}

	if m, err = New("db", "testing"); err != nil {
		t.Fatal(err)
	}

	entryCnt = 0
	m.ForEach(func(key, value string) (end bool) {
		if tm[key] != value {
			t.Fatalf("invalid value, expected %s and received %s", tm[key], value)
		}

		entryCnt++
		return
	})

	if entryCnt != 2 {
		t.Fatalf("invalid entry count, expected %d and received %d", 2, entryCnt)
	}

	if err = m.Txn(func(txn *Txn) (err error) {
		return txn.Put("results", "none")
	}); err != nil {
		t.Fatal(err)
	}

	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
}
