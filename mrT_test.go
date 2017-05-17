package mrT

import (
	//"fmt"
	"testing"
)

func TestMrT(t *testing.T) {
	var (
		m   *MrT
		err error
	)

	if m, err = New("./testing/", "testing"); err != nil {
		t.Fatal(err)
	}

	if err = m.Commit(func(fn WriteFn) (err error) {
		fn([]byte("hello"))
		fn([]byte("world"))
		return
	}); err != nil {
		return
	}

	if err = m.Commit(func(fn WriteFn) (err error) {
		fn([]byte("hello"))
		fn([]byte("world"))
		return
	}); err != nil {
		return
	}

	var entryCount int
	m.ForEach(func(cid string, data []byte) (end bool) {
		entryCount++
		return
	})

	if entryCount != 4 {
		t.Fatalf("invalid entry count, expected %d and received %d", 4, entryCount)
	}

	m.Archive(func(fn WriteFn) (err error) {
		fn([]byte("hello"))
		fn([]byte("world :)"))
		return
	})

	if err = m.Commit(func(fn WriteFn) (err error) {
		fn([]byte("derp"))
		return
	}); err != nil {
		return
	}

	m.Archive(func(fn WriteFn) (err error) {
		fn([]byte("hello"))
		fn([]byte("world :)"))
		fn([]byte("derp"))
		return
	})

	return
}
