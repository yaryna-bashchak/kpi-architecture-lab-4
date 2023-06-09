package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	outFileName = "current-data"
	bufSize     = 8192
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out              *os.File
	outPath          string
	outOffset        int64
	dir              string
	segmentSize      int64
	lastSegmentIndex int
	indexOps         chan IndexOp
	keyPositions     chan *KeyPosition
	putOps           chan entry

	index    hashIndex
	segments []*Segment
}

type Segment struct {
	outOffset int64
	index     hashIndex
	filePath  string
	mu sync.Mutex
}

type IndexOp struct {
	isWrite bool
	key     string
	index   int64
}

type KeyPosition struct {
	segment  *Segment
	position int64
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		dir:          dir,
		segmentSize:  segmentSize,
		segments:     make([]*Segment, 0),
		indexOps:     make(chan IndexOp),
		keyPositions: make(chan *KeyPosition),
		putOps:       make(chan entry),
	}

	err := db.createSegment()
	if err != nil {
		return nil, err
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	db.startIndexRoutine()
	db.startPutRoutine()

	return db, nil
}

func (db *Db) startIndexRoutine() {
	go func() {
		for {
			op := <-db.indexOps
			if op.isWrite {
				db.setKey(op.key, op.index)
			} else {
				segment, position, err := db.getSegmentAndPosition(op.key)
				if err != nil {
					db.keyPositions <- nil
				} else {
					db.keyPositions <- &KeyPosition{
						segment,
						position,
					}
				}
			}
		}
	}()
}

func (db *Db) createSegment() error {
	filePath := db.getNewFileName()
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	newSegment := &Segment{
		filePath: filePath,
		index:    make(hashIndex),
	}

	db.out = f
	db.outOffset = 0
	db.segments = append(db.segments, newSegment)
	db.outPath = filePath

	if len(db.segments) >= 3 {
		go db.compactOldSegments()
	}

	return nil
}

func (db *Db) getPos(key string) *KeyPosition {
	readOp := IndexOp{
		isWrite: false,
		key:     key,
	}
	db.indexOps <- readOp
	return <-db.keyPositions
}

func (db *Db) getNewFileName() string {
	result := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.lastSegmentIndex))
	db.lastSegmentIndex++
	return result
}

func (db *Db) compactOldSegments() {
	filePath := db.getNewFileName()
	newSegment := &Segment{
		filePath: filePath,
		index:    make(hashIndex),
	}
	var offset int64

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return
	}
	defer f.Close()

	lastSegmentIndex := len(db.segments) - 2
	for i := 0; i <= lastSegmentIndex; i++ {
		s := db.segments[i]
		s.mu.Lock()
		for key, index := range s.index {
			if i < lastSegmentIndex && checkKeyInSegments(db.segments[i+1:lastSegmentIndex+1], key) {
				continue
			}

			value, _ := s.getFromSegment(index)
			e := entry{
				key:   key,
				value: value,
			}

			n, err := f.Write(e.Encode())
			if err == nil {
				newSegment.index[key] = offset
				offset += int64(n)
			}
		}
		s.mu.Unlock()
	}

	db.segments = []*Segment{newSegment, db.getLastSegment()}
}

func checkKeyInSegments(segments []*Segment, key string) bool {
	for _, s := range segments {
		s.mu.Lock()
		if _, ok := s.index[key]; ok {
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
	}
	return false
}

func (db *Db) recover() error {
	for _, segment := range db.segments {
		file, err := os.Open(segment.filePath)
		if err != nil {
			return err
		}
		defer file.Close()

		var buf [bufSize]byte
		in := bufio.NewReaderSize(file, bufSize)

		for err == nil {
			header, err := in.Peek(bufSize)
			if err == io.EOF {
				if len(header) == 0 {
					return err
				}
			} else if err != nil {
				return err
			}

			size := binary.LittleEndian.Uint32(header)
			var data []byte

			if size < bufSize {
				data = buf[:size]
			} else {
				data = make([]byte, size)
			}

			n, err := in.Read(data)

			if err != nil {
				return err
			}

			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.setKey(e.key, int64(n))
		}
	}
	return nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) setKey(key string, n int64) {
	db.getLastSegment().mu.Lock()
	defer db.getLastSegment().mu.Unlock()

	db.getLastSegment().index[key] = db.outOffset
	db.outOffset += n
}

func (db *Db) getSegmentAndPosition(key string) (*Segment, int64, error) {
	for i := range db.segments {
		s := db.segments[len(db.segments)-i-1]
		s.mu.Lock()
		pos, ok := s.index[key]
		if ok {
			s.mu.Unlock()
			return s, pos, nil
		}
		s.mu.Unlock()
	}

	return nil, 0, ErrNotFound
}

func (db *Db) Get(key string) (string, error) {
	keyPos := db.getPos(key)
	if keyPos == nil {
		return "", ErrNotFound
	}
	value, err := keyPos.segment.getFromSegment(keyPos.position)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) getLastSegment() *Segment {
	return db.segments[len(db.segments)-1]
}

func (db *Db) startPutRoutine() {
	go func() {
		for {
			entry := <-db.putOps
			length := entry.getLength()

			stat, err := db.out.Stat()
			if err != nil {
				entry.done <- err
				continue
			}

			if stat.Size()+length > db.segmentSize {
				if err := db.createSegment(); err != nil {
					entry.done <- err
					continue
				}
			}

			n, err := db.out.Write(entry.Encode())
			if err == nil {
				db.indexOps <- IndexOp{
					isWrite: true,
					key:     entry.key,
					index:   int64(n),
				}
			}
			entry.done <- nil
		}
	}()
}

func (db *Db) Put(key, value string) error {
	done := make(chan error)
	entry := entry{
		key:   key,
		value: value,
		done: done,
	}
	db.putOps <- entry
	return <-done
}

func (s *Segment) getFromSegment(position int64) (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}

	return value, nil
}