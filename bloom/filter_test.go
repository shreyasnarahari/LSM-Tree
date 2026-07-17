package bloom

import (
	"fmt"
	"testing"
)

func TestFilterBasic(t *testing.T) {
	bf := New(1000, 0.01)
	for i := 0; i < 1000; i++ {
		bf.Add([]byte(fmt.Sprintf("key-%04d", i)))
	}

	// All inserted keys must be found (no false negatives).
	for i := 0; i < 1000; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("key-%04d", i))) {
			t.Fatalf("false negative for key-%04d", i)
		}
	}
}

func TestFilterFalsePositiveRate(t *testing.T) {
	const n = 100_000
	bf := New(n, 0.01)
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("bloom-%08d", i)))
	}

	// Probe keys that were never inserted.
	falsePositives := 0
	const probes = 100_000
	for i := 0; i < probes; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("absent-%08d", i))) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / float64(probes)
	t.Logf("false positive rate: %.4f%% (%d / %d)", rate*100, falsePositives, probes)
	// Allow up to 2% (generous margin over target 1%).
	if rate > 0.02 {
		t.Fatalf("false positive rate %.4f exceeds 2%%", rate)
	}
}

func TestFilterSerialization(t *testing.T) {
	bf := New(500, 0.01)
	for i := 0; i < 500; i++ {
		bf.Add([]byte(fmt.Sprintf("ser-%04d", i)))
	}

	data := bf.MarshalBinary()
	bf2 := Unmarshal(data)

	// All keys must survive the roundtrip.
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("ser-%04d", i))
		if !bf2.MayContain(key) {
			t.Fatalf("false negative after deserialization for key %d", i)
		}
	}
	if bf2.NumBits != bf.NumBits || bf2.NumHashes != bf.NumHashes {
		t.Fatalf("params mismatch: got (%d,%d), want (%d,%d)",
			bf2.NumBits, bf2.NumHashes, bf.NumBits, bf.NumHashes)
	}
}

func TestFilterEmpty(t *testing.T) {
	bf := New(100, 0.01)

	// An empty filter should not report any key as present.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		if bf.MayContain(key) {
			t.Fatalf("empty filter reports key-%04d as present", i)
		}
	}
}

func TestFilterSingleItem(t *testing.T) {
	bf := New(1, 0.01)
	bf.Add([]byte("only-key"))

	if !bf.MayContain([]byte("only-key")) {
		t.Fatal("false negative for the only inserted key")
	}
}

// Benchmarks

func BenchmarkFilterAdd(b *testing.B) {
	bf := New(b.N, 0.01)
	key := []byte("benchmark-bloom-key-12345")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bf.Add(key)
	}
}

func BenchmarkFilterMayContain(b *testing.B) {
	const n = 100_000
	bf := New(n, 0.01)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("bloom-bench-%08d", i))
		bf.Add(keys[i])
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bf.MayContain(keys[i%n])
	}
}
