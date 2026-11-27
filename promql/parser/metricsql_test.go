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

package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeepMetricNames(t *testing.T) {
	for _, tc := range []struct {
		input string
		// kept are the functions that keep their metric names.
		kept []string
		// out is the printed query, which must parse to the same query.
		out string
	}{
		{
			input: `rate(m[5m]) keep_metric_names`,
			kept:  []string{"rate"},
			out:   `rate(m[5m]) keep_metric_names`,
		},
		{
			input: `sum by (job) (last_over_time(m[8d]) keep_metric_names)`,
			kept:  []string{"last_over_time"},
			out:   `sum by (job) (last_over_time(m[8d]) keep_metric_names)`,
		},
		{
			input: `label_replace(sum_over_time({__name__=~"a|b"}[1h]) keep_metric_names, "p", "$1", "__name__", "(.*)")`,
			kept:  []string{"sum_over_time"},
			out:   `label_replace(sum_over_time({__name__=~"a|b"}[1h]) keep_metric_names, "p", "$1", "__name__", "(.*)")`,
		},
		{
			input: `abs(rate(m[5m]) keep_metric_names) keep_metric_names`,
			kept:  []string{"abs", "rate"},
			out:   `abs(rate(m[5m]) keep_metric_names) keep_metric_names`,
		},
		{
			// Only the call before the keyword keeps its names.
			input: `abs(rate(m[5m])) keep_metric_names + rate(n[1m])`,
			kept:  []string{"abs"},
			out:   `abs(rate(m[5m])) keep_metric_names + rate(n[1m])`,
		},
		{
			input: `(rate(m[5m]) keep_metric_names)`,
			kept:  []string{"rate"},
			out:   `(rate(m[5m]) keep_metric_names)`,
		},
		{
			input: "rate(m[5m]) # comment\n keep_metric_names",
			kept:  []string{"rate"},
			out:   `rate(m[5m]) keep_metric_names`,
		},
		{
			// A metric of that name is still a metric.
			input: `keep_metric_names + rate(keep_metric_names[5m])`,
			out:   `keep_metric_names + rate(keep_metric_names[5m])`,
		},
		{
			input: `m{keep_metric_names="x"}`,
			out:   `m{keep_metric_names="x"}`,
		},
	} {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := ParseExpr(tc.input)
			require.NoError(t, err)
			var kept []string
			Inspect(expr, func(n Node, _ []Node) error {
				if c, ok := n.(*Call); ok && c.KeepMetricNames {
					kept = append(kept, c.Func.Name)
				}
				return nil
			})
			require.ElementsMatch(t, tc.kept, kept)
			require.Equal(t, tc.out, expr.String())

			again, err := ParseExpr(expr.String())
			require.NoError(t, err)
			require.Equal(t, expr.String(), again.String())
		})
	}
}

func TestKeepMetricNamesErrors(t *testing.T) {
	for input, want := range map[string]string{
		`sum(m) keep_metric_names`:                        "keep_metric_names must follow a function call",
		`(m + n) keep_metric_names`:                       "keep_metric_names must follow a function call",
		`sum by (job) keep_metric_names (m)`:              "keep_metric_names must follow a function call",
		`rate(m[5m]) keep_metric_names keep_metric_names`: "unexpected identifier",
		`m keep_metric_names`:                             "unexpected identifier",
	} {
		_, err := ParseExpr(input)
		require.ErrorContains(t, err, want, input)
	}
}

func TestScalarClamp(t *testing.T) {
	for input, want := range map[string]string{
		`clamp_max((time() - 1790851020) / 604800, 1)`: `scalar(clamp_max(vector((time() - 1.79085102e+09) / 604800), 1))`,
		`clamp_min(scalar(sum(m)), 1)`:                 `scalar(clamp_min(vector(scalar(sum(m))), 1))`,
		`clamp(1, 0, 2)`:                               `scalar(clamp(vector(1), 0, 2))`,
		`clamp_min(clamp_max(time(), 5), 1)`:           `scalar(clamp_min(vector(scalar(clamp_max(vector(time()), 5))), 1))`,
		`m / clamp_max(time() - 5, 1)`:                 `m / scalar(clamp_max(vector(time() - 5), 1))`,
		// A vector argument is as in Prometheus.
		`clamp_max(m, 1)`: `clamp_max(m, 1)`,
	} {
		expr, err := ParseExpr(input)
		require.NoError(t, err, input)
		require.Equal(t, want, expr.String(), input)
		if want[:7] == "scalar(" {
			require.Equal(t, ValueTypeScalar, expr.Type(), input)
		}
		_, err = ParseExpr(expr.String())
		require.NoError(t, err)
	}

	// The rewrite gives a scalar, which is not an instant vector.
	_, err := ParseExpr(`sum(clamp_max(time(), 1))`)
	require.ErrorContains(t, err, "expected type instant vector in aggregation expression, got scalar")
	// An error in the argument comes once.
	_, err = ParseExpr(`clamp_max(time() + "a", 1)`)
	require.Error(t, err)
	require.Len(t, err.(ParseErrors), 1, err)
	// The other arguments are checked as before.
	_, err = ParseExpr(`clamp_max(time(), m)`)
	require.ErrorContains(t, err, "expected type scalar")
}
