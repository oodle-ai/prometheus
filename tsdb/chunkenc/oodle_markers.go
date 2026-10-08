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

	"github.com/prometheus/prometheus/model/value"
)

// oodleResetNaN is the counter reset marker Oodle writes into histogram
// chunks (oodle/util/mathx.ResetNaN). The two values must stay equal.
const oodleResetNaN uint64 = 0x7ff0000000000033

// isBucketlessMarker reports whether a histogram sample with this sum
// was written with no buckets. A stale marker is, and so is the Oodle
// reset marker. The iterators read no bucket deltas for such a sample.
// Without the reset marker here, a reset marker after other samples
// was read with the bucket deltas of the next sample, or past the end
// of the chunk, which fails the query with io.EOF.
func isBucketlessMarker(sum float64) bool {
	return value.IsStaleNaN(sum) || math.Float64bits(sum) == oodleResetNaN
}
