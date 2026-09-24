// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVarbitInt(t *testing.T) {
	numbers := []int64{
		math.MinInt64,
		-36028797018963968, -36028797018963967,
		-16777216, -16777215,
		-131072, -131071,
		-2048, -2047,
		-256, -255,
		-32, -31,
		-4, -3,
		-1, 0, 1,
		4, 5,
		32, 33,
		256, 257,
		2048, 2049,
		131072, 131073,
		16777216, 16777217,
		36028797018963968, 36028797018963969,
		math.MaxInt64,
	}

	bs := bstream{}

	for _, n := range numbers {
		putVarbitInt(&bs, n)
	}

	bsr := newBReader(bs.bytes())

	for _, want := range numbers {
		got, err := readVarbitInt(&bsr)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestVarbitUint(t *testing.T) {
	numbers := []uint64{
		0, 1,
		7, 8,
		63, 64,
		511, 512,
		4095, 4096,
		262143, 262144,
		33554431, 33554432,
		72057594037927935, 72057594037927936,
		math.MaxUint64,
	}

	bs := bstream{}

	for _, n := range numbers {
		putVarbitUint(&bs, n)
	}

	bsr := newBReader(bs.bytes())

	for _, want := range numbers {
		got, err := readVarbitUint(&bsr)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

// TestVarbitIntRandomRoundTrip covers the payload sizes and
// buffer positions the fast path takes: values of every size
// class in random order, so the prefix lands at every bit
// offset of the read buffer.
func TestVarbitIntRandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	limits := []int64{0, 1, 3, 4, 31, 32, 255, 256, 2047, 2048, 131071, 131072,
		16777215, 16777216, 36028797018963967, 36028797018963968, math.MaxInt64}
	for round := 0; round < 50; round++ {
		var numbers []int64
		for i := 0; i < 2000; i++ {
			lim := limits[rng.Intn(len(limits))]
			v := lim
			if lim > 0 && lim < math.MaxInt64 {
				v = rng.Int63n(lim + 1)
			}
			if rng.Intn(2) == 0 {
				v = -v
			}
			numbers = append(numbers, v)
		}
		bs := bstream{}
		for _, n := range numbers {
			putVarbitInt(&bs, n)
		}
		bsr := newBReader(bs.bytes())
		for i, want := range numbers {
			got, err := readVarbitInt(&bsr)
			require.NoError(t, err, "round %d value %d", round, i)
			require.Equal(t, want, got, "round %d value %d", round, i)
		}

		// The same stream through readVarbitInts, in runs of
		// random length with single reads in between, so the
		// fast path starts and hands over at every buffer
		// state, and the run that reaches the end of the
		// stream takes the slow path for its last codes. The
		// destination starts non-zero to check that the values
		// are added, not stored.
		bsr = newBReader(bs.bytes())
		for i := 0; i < len(numbers); {
			if rng.Intn(4) == 0 {
				got, err := readVarbitInt(&bsr)
				require.NoError(t, err, "round %d value %d", round, i)
				require.Equal(t, numbers[i], got, "round %d value %d", round, i)
				i++
				continue
			}
			n := 1 + rng.Intn(50)
			if i+n > len(numbers) {
				n = len(numbers) - i
			}
			vals := make([]int64, n)
			for j := range vals {
				vals[j] = int64(j) - 7
			}
			require.NoError(t, readVarbitInts(&bsr, vals), "round %d values %d..%d", round, i, i+n)
			for j := range vals {
				require.Equal(t, numbers[i+j]+int64(j)-7, vals[j], "round %d value %d", round, i+j)
			}
			i += n
		}
	}
}

// TestVarbitIntsZeroRuns covers the step that takes a run of
// zeros at once. Runs are longer than the read buffer, cross
// every top up, and end at the end of the slice, so a read must
// stop inside a run and leave the rest of it for the next read.
// The stream ends in a run, which the last bytes read through
// the slow path. The padding bits of the last byte are zeros as
// well, so a read past the end does not fail; the read must only
// take the values asked for.
func TestVarbitIntsZeroRuns(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	limits := []int64{1, 4, 32, 256, 2048, 131072, 16777216, 36028797018963968, math.MaxInt64}
	for round := 0; round < 50; round++ {
		var numbers []int64
		for len(numbers) < 3000 {
			for n := rng.Intn(200); n > 0; n-- {
				numbers = append(numbers, 0)
			}
			v := rng.Int63n(limits[rng.Intn(len(limits))]) + 1
			if rng.Intn(2) == 0 {
				v = -v
			}
			numbers = append(numbers, v)
		}
		for n := 1 + rng.Intn(100); n > 0; n-- {
			numbers = append(numbers, 0)
		}

		bs := bstream{}
		for _, n := range numbers {
			putVarbitInt(&bs, n)
		}
		bsr := newBReader(bs.bytes())
		for i := 0; i < len(numbers); {
			n := min(1+rng.Intn(80), len(numbers)-i)
			vals := make([]int64, n)
			for j := range vals {
				vals[j] = int64(j) + 3
			}
			require.NoError(t, readVarbitInts(&bsr, vals), "round %d values %d..%d", round, i, i+n)
			for j := range vals {
				require.Equal(t, numbers[i+j]+int64(j)+3, vals[j], "round %d value %d", round, i+j)
			}
			i += n
		}
	}
}

// TestVarbitIntsTruncatedStream checks that on a stream cut
// short the fast path gives what the slow one gives, value for
// value and error for error. A bit stream carries no count, so
// the slow reader itself reads padding as zero codes and only
// fails when it runs out of bytes inside a code; the chunk
// relies on its sample count. A run of zero length reads
// nothing.
func TestVarbitIntsTruncatedStream(t *testing.T) {
	bs := bstream{}
	for _, v := range []int64{5, -1000, 1 << 40, 3, 0, 0, 1 << 60, -(1 << 62), 7} {
		putVarbitInt(&bs, v)
	}
	full := bs.bytes()

	bsr := newBReader(full)
	require.NoError(t, readVarbitInts(&bsr, nil))
	require.NoError(t, readVarbitInts(&bsr, []int64{}))
	vals := make([]int64, 9)
	require.NoError(t, readVarbitInts(&bsr, vals))
	require.Equal(t, []int64{5, -1000, 1 << 40, 3, 0, 0, 1 << 60, -(1 << 62), 7}, vals)

	for cut := 1; cut < len(full); cut++ {
		bsr := newBReader(full[:cut])
		vals := make([]int64, 9)
		fastErr := readVarbitInts(&bsr, vals)

		ref := newBReader(full[:cut])
		want := make([]int64, 9)
		var slowErr error
		for i := range want {
			want[i], slowErr = readVarbitInt(&ref)
			if slowErr != nil {
				want[i] = 0
				break
			}
		}
		require.Equal(t, slowErr, fastErr, "cut at %d bytes", cut)
		require.Equal(t, want, vals, "cut at %d bytes", cut)
	}
}

// FuzzReadVarbitIntsRoundTrip encodes arbitrary values and reads
// them back through readVarbitInts in runs of arbitrary length,
// with single slow reads between them, so every code, the 64 bit
// one included, is read at every alignment and buffer state. The
// values have to come back exactly.
func FuzzReadVarbitIntsRoundTrip(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0x80, 1, 2, 3, 4, 5, 6, 7, 8}, []byte{3, 0, 1})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, []byte{1})
	f.Fuzz(func(t *testing.T, raw, runs []byte) {
		var values []int64
		for i := 0; i+8 <= len(raw); i += 8 {
			v := int64(binary.LittleEndian.Uint64(raw[i:]))
			// Spread the values over every size class.
			switch raw[i] % 4 {
			case 0:
				v >>= 60
			case 1:
				v >>= 40
			case 2:
				v >>= 20
			}
			values = append(values, v)
		}
		bs := bstream{}
		for _, v := range values {
			putVarbitInt(&bs, v)
		}
		r := newBReader(bs.bytes())
		for i, k := 0, 0; i < len(values); k++ {
			n := 1
			if len(runs) > 0 {
				n = int(runs[k%len(runs)])
			}
			if n == 0 {
				v, err := readVarbitInt(&r)
				require.NoError(t, err)
				require.Equal(t, values[i], v, "value %d", i)
				i++
				continue
			}
			n = min(n, len(values)-i)
			got := make([]int64, n)
			require.NoError(t, readVarbitInts(&r, got))
			require.Equal(t, values[i:i+n], got, "values %d..%d", i, i+n)
			i += n
		}
	})
}

// FuzzReadVarbitInts feeds arbitrary bytes, corrupt and cut
// short, to the fast decoder: it must not panic or read past the
// stream, and it must agree with the slow decoder wherever that
// one decodes whole codes. On a code cut short the slow decoder
// returns an unspecified value without an error (readBits does
// not notice it ran out), and the fast one, which loads the
// buffer in other steps, may return another, so the values are
// compared only up to the first code that runs past the end.
func FuzzReadVarbitInts(f *testing.F) {
	bs := bstream{}
	for _, v := range []int64{0, 1, -1, 4, -3, 32, 256, 2048, 131072, 16777216, 1 << 40, math.MinInt64, math.MaxInt64} {
		putVarbitInt(&bs, v)
	}
	f.Add(bs.bytes(), uint8(13))
	f.Add([]byte{0xff, 0xff, 0xff}, uint8(4))
	f.Fuzz(func(t *testing.T, data []byte, n uint8) {
		slow := newBReader(data)
		want := make([]int64, 0, n)
		totalBits := 8 * len(data)
		for i := 0; i < int(n); i++ {
			before := 8*slow.streamOffset - int(slow.valid)
			v, err := readVarbitInt(&slow)
			after := 8*slow.streamOffset - int(slow.valid)
			if err != nil || after > totalBits || after < before {
				break
			}
			want = append(want, v)
		}
		fast := newBReader(data)
		got := make([]int64, n)
		_ = readVarbitInts(&fast, got) // must not panic
		require.Equal(t, want, got[:len(want)])
	})
}
