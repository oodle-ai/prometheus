// Copyright 2026 The Prometheus Authors
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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
)

// wideCumulativeChunk builds a chunk of `numSamples` cumulative
// histograms with `width` buckets in one span. Bucket counts
// grow by a small random amount per sample, so the deltas of
// deltas are small and the varbit prefixes short, as with a
// counter histogram whose rates change a little all the time.
func wideCumulativeChunk(tb testing.TB, numSamples, width int) *HistogramChunk {
	return cumulativeChunk(tb, numSamples, width, true)
}

// steadyCumulativeChunk builds a chunk like wideCumulativeChunk,
// but each bucket grows by the same amount in every sample. From
// the third sample on, every delta of deltas is zero, as with a
// counter histogram whose rates hold steady, so the reader takes
// runs of zero codes.
func steadyCumulativeChunk(tb testing.TB, numSamples, width int) *HistogramChunk {
	return cumulativeChunk(tb, numSamples, width, false)
}

func cumulativeChunk(tb testing.TB, numSamples, width int, jitter bool) *HistogramChunk {
	tb.Helper()
	rng := rand.New(rand.NewSource(1))
	chk := NewHistogramChunk()
	app, err := chk.Appender()
	require.NoError(tb, err)

	counts := make([]int64, width)
	for s := 0; s < numSamples; s++ {
		h := &histogram.Histogram{
			Schema:        5,
			ZeroThreshold: 1e-128,
			PositiveSpans: []histogram.Span{{Offset: 10, Length: uint32(width)}},
		}
		var prev int64
		for i := range counts {
			x := float64(i-width/2) / float64(width/6)
			counts[i] += int64(math.Exp(-x*x/2) * 20)
			if jitter {
				counts[i] += int64(rng.Intn(3))
			}
			h.PositiveBuckets = append(h.PositiveBuckets, counts[i]-prev)
			h.Count += uint64(counts[i])
			prev = counts[i]
		}
		h.Sum = float64(h.Count) * 12.5
		_, _, _, err := app.AppendHistogram(nil, int64(s)*15_000, h, true)
		require.NoError(tb, err)
	}
	return chk
}

// BenchmarkHistogramIteratorNext decodes every sample of a chunk
// and reads it in one of the forms that callers use: a float
// histogram into a destination that is used again, a float
// histogram without a destination, and an integer histogram into
// a destination that is used again.
func BenchmarkHistogramIteratorNext(b *testing.B) {
	chunks := []struct {
		name  string
		build func(testing.TB, int, int) *HistogramChunk
	}{
		{"random", wideCumulativeChunk},
		{"steady", steadyCumulativeChunk},
	}
	reads := []struct {
		name string
		read func(it Iterator, h *histogram.Histogram, fh *histogram.FloatHistogram) (*histogram.Histogram, *histogram.FloatHistogram)
	}{
		{"float", func(it Iterator, h *histogram.Histogram, fh *histogram.FloatHistogram) (*histogram.Histogram, *histogram.FloatHistogram) {
			_, fh = it.AtFloatHistogram(fh)
			return h, fh
		}},
		{"float_nil", func(it Iterator, h *histogram.Histogram, _ *histogram.FloatHistogram) (*histogram.Histogram, *histogram.FloatHistogram) {
			_, fh := it.AtFloatHistogram(nil)
			return h, fh
		}},
		{"int", func(it Iterator, h *histogram.Histogram, fh *histogram.FloatHistogram) (*histogram.Histogram, *histogram.FloatHistogram) {
			_, h = it.AtHistogram(h)
			return h, fh
		}},
	}
	const numSamples = 240
	for _, c := range chunks {
		for _, width := range []int{6, 30, 64, 225, 677} {
			chk := c.build(b, numSamples, width)
			for _, r := range reads {
				b.Run(fmt.Sprintf("%s/%dx%d/%s", c.name, numSamples, width, r.name), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(chk.Bytes())))
					var it Iterator
					h := &histogram.Histogram{}
					fh := &histogram.FloatHistogram{}
					for i := 0; i < b.N; i++ {
						it = chk.Iterator(it)
						n := 0
						for it.Next() != ValNone {
							h, fh = r.read(it, h, fh)
							n++
						}
						if n != numSamples {
							b.Fatalf("read %d samples, want %d", n, numSamples)
						}
					}
				})
			}
		}
	}
}
