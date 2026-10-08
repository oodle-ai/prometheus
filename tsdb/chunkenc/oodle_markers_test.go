// Copyright The Prometheus Authors
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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
)

// As wide as a production latency series. With a few buckets a
// missing bucket read can fit in the padding at the end of the
// chunk and not show.
const markerTestWidth = 74

func markerTestHistogram(i int) *histogram.Histogram {
	h := &histogram.Histogram{
		Schema:          5,
		ZeroThreshold:   1e-128,
		PositiveSpans:   []histogram.Span{{Offset: 3, Length: markerTestWidth}},
		PositiveBuckets: make([]int64, markerTestWidth),
	}
	h.PositiveBuckets[0] = int64(i + 1)
	h.Count = uint64(i + 1)
	h.Sum = float64(i)
	return h
}

// A marker after other samples is read back as a marker, with no
// buckets, and every sample of the chunk is read. A reset marker used
// to be read with bucket deltas it does not hold, which failed with
// io.EOF at the end of the chunk.
func TestHistogramMarkerAfterSamples(t *testing.T) {
	for name, sum := range map[string]float64{
		"reset": math.Float64frombits(oodleResetNaN),
		"stale": math.Float64frombits(value.StaleNaN),
	} {
		t.Run(name, func(t *testing.T) {
			c := NewHistogramChunk()
			app, err := c.Appender()
			require.NoError(t, err)
			for i := 0; i < 10; i++ {
				newChunk, _, _, err := app.AppendHistogram(nil, int64(i*1000), markerTestHistogram(i), false)
				require.NoError(t, err)
				require.Nil(t, newChunk)
			}
			newChunk, _, _, err := app.AppendHistogram(nil, 10_000, &histogram.Histogram{Sum: sum}, false)
			require.NoError(t, err)
			require.Nil(t, newChunk, "a marker goes in the chunk")

			it := c.Iterator(nil)
			n := 0
			for it.Next() == ValHistogram {
				ts, h := it.AtHistogram(nil)
				if n == 10 {
					require.Equal(t, int64(10_000), ts)
					require.Equal(t, math.Float64bits(sum), math.Float64bits(h.Sum))
					require.Empty(t, h.PositiveBuckets)
				} else {
					require.Equal(t, uint64(n+1), h.Count)
				}
				n++
			}
			require.NoError(t, it.Err())
			require.Equal(t, 11, n)
			require.Equal(t, 11, c.NumSamples())

			// A sample after the marker starts a new chunk.
			newChunk, _, _, err = app.AppendHistogram(nil, 11_000, markerTestHistogram(0), false)
			require.NoError(t, err)
			require.NotNil(t, newChunk)
		})
	}
}

func TestFloatHistogramResetMarkerAfterSamples(t *testing.T) {
	c := NewFloatHistogramChunk()
	app, err := c.Appender()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, _, _, err := app.AppendFloatHistogram(nil, int64(i*1000), markerTestHistogram(i).ToFloat(nil), false)
		require.NoError(t, err)
	}
	newChunk, _, _, err := app.AppendFloatHistogram(nil, 10_000, &histogram.FloatHistogram{Sum: math.Float64frombits(oodleResetNaN)}, false)
	require.NoError(t, err)
	require.Nil(t, newChunk)

	it := c.Iterator(nil)
	n := 0
	for it.Next() == ValFloatHistogram {
		_, fh := it.AtFloatHistogram(nil)
		if n == 10 {
			require.Equal(t, oodleResetNaN, math.Float64bits(fh.Sum))
			require.Empty(t, fh.PositiveBuckets)
		}
		n++
	}
	require.NoError(t, it.Err())
	require.Equal(t, 11, n)
}
