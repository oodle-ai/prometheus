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
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

// This file holds the parts of VictoriaMetrics' MetricsQL that the
// parser accepts, so that queries that move from VictoriaMetrics keep
// working. Both are additions: a query that Prometheus accepts parses
// as before.

// keepMetricNamesKeyword follows the closing parenthesis of a function
// call in MetricsQL, for example rate(m[5m]) keep_metric_names. The
// function then keeps the metric name of each series, which it drops
// by default.
const keepMetricNamesKeyword = "keep_metric_names"

// keepMetricNamesAt is a keep_metric_names keyword that the parser
// read. closing is the end of the parenthesis before it.
type keepMetricNamesAt struct {
	closing posrange.Pos
	pos     posrange.PositionRange
}

// isKeepMetricNames reports whether the item is the keep_metric_names
// keyword. The keyword comes only after a closing parenthesis. There,
// no other identifier is valid, so a metric with that name elsewhere
// still parses as a metric.
func (p *parser) isKeepMetricNames(item Item) bool {
	return p.lastType == RIGHT_PAREN && item.Typ == IDENTIFIER && item.Val == keepMetricNamesKeyword
}

// setKeepMetricNames sets KeepMetricNames on the function calls that
// end where a keep_metric_names keyword starts. The keyword does not
// go to the generated parser, so the grammar stays as in Prometheus.
// A keyword after a parenthesis that does not close a function call,
// for example after an aggregation, is an error.
func (p *parser) setKeepMetricNames(expr Expr) {
	if len(p.keepMetricNames) == 0 || expr == nil {
		return
	}
	calls := map[posrange.Pos]*Call{}
	Inspect(expr, func(node Node, _ []Node) error {
		if call, ok := node.(*Call); ok {
			calls[call.PosRange.End] = call
		}
		return nil
	})
	for _, k := range p.keepMetricNames {
		call, ok := calls[k.closing]
		if !ok {
			p.addParseErrf(k.pos, "%s must follow a function call", keepMetricNamesKeyword)
			continue
		}
		call.KeepMetricNames = true
	}
}

// scalarClampFunctions are the functions that MetricsQL also runs on a
// number, for example clamp_max(time() - 1700000000, 3600).
var scalarClampFunctions = map[string]bool{
	"clamp":     true,
	"clamp_min": true,
	"clamp_max": true,
}

// rewriteScalarClamps changes each clamp function whose first argument
// is a scalar into scalar(clamp(vector(x), ...)). Prometheus accepts
// only an instant vector there. MetricsQL makes a series without
// labels of the number, and a binary operation uses such a series as a
// scalar. The rewrite gives the same value, and the result is a scalar,
// so a binary operation with a vector applies it to every series, as
// in MetricsQL. The rewrite uses only Prometheus functions, so any
// engine and the printed query give the same answer.
//
// It runs before the type check, which must see each node once: the
// check of a binary expression changes the expression. The type of an
// argument therefore comes from Type(), and the children are changed
// first, so that a clamp of a clamp of a number is a scalar too.
func (p *parser) rewriteScalarClamps(node Node) {
	if node == nil {
		return
	}
	for _, child := range Children(node) {
		p.rewriteScalarClamps(child)
	}
	n, ok := node.(*Call)
	if !ok || n.Func == nil || !scalarClampFunctions[n.Func.Name] || len(n.Args) == 0 || n.KeepMetricNames {
		return
	}
	if n.Args[0].Type() != ValueTypeScalar {
		return
	}
	vectorFn, ok1 := getFunction("vector", p.functions)
	scalarFn, ok2 := getFunction("scalar", p.functions)
	if !ok1 || !ok2 {
		return
	}
	pos := n.PosRange
	arg := n.Args[0]
	vector := &Call{Func: vectorFn, Args: Expressions{arg}, PosRange: arg.PositionRange()}
	inner := &Call{Func: n.Func, Args: append(Expressions{vector}, n.Args[1:]...), PosRange: pos}
	*n = Call{Func: scalarFn, Args: Expressions{inner}, PosRange: pos}
}
