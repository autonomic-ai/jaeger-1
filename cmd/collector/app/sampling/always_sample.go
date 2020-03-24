package sampling

import (
	"github.com/jaegertracing/jaeger/model"
	"go.uber.org/zap"
)

type alwaysSample struct {
	logger *zap.Logger
}

var _ PolicyEvaluator = (*alwaysSample)(nil)

// NewAlwaysSample creates a policy evaluator the samples all traces.
func NewAlwaysSample(logger *zap.Logger) PolicyEvaluator {
	return &alwaysSample{
		logger: logger,
	}
}

// OnLateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (as *alwaysSample) OnLateArrivingSpans(earlyDecision Decision, spans []*model.Span) error {
	as.logger.Debug("Triggering action for late arriving spans in always-sample filter")
	return nil
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (as *alwaysSample) Evaluate(trace *TraceData) (Decision, error) {
	as.logger.Debug("Evaluating spans in always-sample filter")
	return Sampled, nil
}

// OnDroppedSpans is called when the trace needs to be dropped, due to memory
// pressure, before the decision_wait time has been reached.
func (as *alwaysSample) OnDroppedSpans(trace *TraceData) (Decision, error) {
	as.logger.Debug("Triggering action for dropped spans in always-sample filter")
	return Sampled, nil
}
