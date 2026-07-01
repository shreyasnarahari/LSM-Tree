package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/shreyas/lsmtree/memtable"
)

func TestIterator(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "iter.sst")

	mt := memtable.New(1 << 30)
	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		tomb := i%10 == 0 // every 10th is a tombstone

		if tomb {
			mt.PutWithTimestamp(key, nil, uint64(i), true)
		} else {
			mt.PutWithTimestamp(key, val, uint64(i), false)
		}
	}

	if err := Build(path, mt.NewIterator()); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	it := reader.NewIterator()
	defer it.Close()

	count := 0
	for it.Valid() {
		wantKey := fmt.Sprintf("key-%06d", count)
		if string(it.Key()) != wantKey {
			t.Fatalf("step %d: got key %q, want %q", count, it.Key(), wantKey)
		}

		tomb := count%10 == 0
		if it.Tombstone() != tomb {
			t.Fatalf("step %d: got tombstone %v, want %v", count, it.Tombstone(), tomb)
		}

		if !tomb {
			wantVal := fmt.Sprintf("val-%06d", count)
			if string(it.Value()) != wantVal {
				t.Fatalf("step %d: got val %q, want %q", count, it.Value(), wantVal)
			}
		}

		if it.Timestamp() != uint64(count) {
			t.Fatalf("step %d: got ts %d, want %d", count, it.Timestamp(), count)
		}

		it.Next()
		count++
	}

	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	if count != n {
		t.Fatalf("iterated %d entries, want %d", count, n)
	}
}
