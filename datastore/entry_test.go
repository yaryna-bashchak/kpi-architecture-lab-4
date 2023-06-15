package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{"key", "value", nil, make(chan error)}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	e := entry{"key", "test-value", nil, make(chan error)}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "test-value" {
		t.Errorf("Got bat value [%s]", v)
	}
}

func TestCheckHashSum(t *testing.T) {
	e := entry{"key", "test-value", nil, make(chan error)}

	sumLength := len(e.key) + len(e.value) + 12
	sumData := e.Encode()[:sumLength]
	expectedSum := sha1.Sum(sumData)

	data := e.Encode()
	newEntry := entry{}
	newEntry.Decode(data)

	if bytes.Compare(newEntry.sum, expectedSum[:]) != 0 {
		t.Errorf("Check hash sum. Expected: %v, Got: %v", expectedSum, newEntry.sum)
	}
}
