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
// counter histogram in steady state.
func wideCumulativeChunk(b testing.TB, numSamples, width int) *HistogramChunk {
	b.Helper()
	rng := rand.New(rand.NewSource(1))
	chk := NewHistogramChunk()
	app, err := chk.Appender()
	require.NoError(b, err)

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
			counts[i] += int64(math.Exp(-x*x/2)*20) + int64(rng.Intn(3))
			h.PositiveBuckets = append(h.PositiveBuckets, counts[i]-prev)
			h.Count += uint64(counts[i])
			prev = counts[i]
		}
		h.Sum = float64(h.Count) * 12.5
		_, _, _, err := app.AppendHistogram(nil, int64(s)*15_000, h, true)
		require.NoError(b, err)
	}
	return chk
}

func BenchmarkHistogramIteratorNext(b *testing.B) {
	for _, width := range []int{6, 30, 64, 225, 677} {
		const numSamples = 240
		chk := wideCumulativeChunk(b, numSamples, width)
		b.Run(fmt.Sprintf("%dx%d", numSamples, width), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(chk.Bytes())))
			var it Iterator
			fh := &histogram.FloatHistogram{}
			for i := 0; i < b.N; i++ {
				it = chk.Iterator(it)
				n := 0
				for it.Next() != ValNone {
					_, fh = it.AtFloatHistogram(fh)
					n++
				}
				if n != numSamples {
					b.Fatalf("read %d samples, want %d", n, numSamples)
				}
			}
		})
	}
}
