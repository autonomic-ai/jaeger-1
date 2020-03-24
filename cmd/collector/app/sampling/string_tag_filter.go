// Copyright 2019, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sampling

import (
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"go.uber.org/zap"
)

type stringAttributeFilter struct {
	key    string
	values map[string]struct{}
	logger *zap.Logger
}

var _ PolicyEvaluator = (*stringAttributeFilter)(nil)

// NewStringAttributeFilter creates a policy evaluator that samples all traces with
// the given attribute in the given numeric range.
func NewStringAttributeFilter(logger *zap.Logger, key string, values []string) PolicyEvaluator {
	valuesMap := make(map[string]struct{})
	for _, value := range values {
		if value != "" {
			valuesMap[value] = struct{}{}
		}
	}
	return &stringAttributeFilter{
		key:    key,
		values: valuesMap,
		logger: logger,
	}
}

// OnLateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (saf *stringAttributeFilter) OnLateArrivingSpans(earlyDecision Decision, spans []*model.Span) error {
	saf.logger.Debug("Triggering action for late arriving spans in string-tag filter")
	return nil
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (saf *stringAttributeFilter) Evaluate(trace *TraceData) (Decision, error) {
	saf.logger.Debug("Evaluting spans in string-tag filter")
	trace.Lock()
	batches := trace.ReceivedBatches
	trace.Unlock()
	for _, batch := range batches {
		for _, span := range batch.Spans {
			if span == nil || span.Tags == nil {
				continue
			}

			for _, tag := range span.Tags {
				if saf.key == tag.Key {
					if _, ok := saf.values[tag.VStr]; ok {
						fmt.Println("match")
						fmt.Println(tag.VStr)
						return Sampled, nil
					}
				}
			}
		}
	}
	fmt.Println("not match")
	return NotSampled, nil
}

// OnDroppedSpans is called when the trace needs to be dropped, due to memory
// pressure, before the decision_wait time has been reached.
func (saf *stringAttributeFilter) OnDroppedSpans(trace *TraceData) (Decision, error) {
	saf.logger.Debug("Triggering action for dropped spans in string-tag filter")
	return NotSampled, nil
}
