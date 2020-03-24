// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
//
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

package app

import (
	"fmt"
	"sync"
	"time"

	tchannel "github.com/uber/tchannel-go"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/collector/app/processor"
	"github.com/jaegertracing/jaeger/cmd/collector/app/sampling"
	"github.com/jaegertracing/jaeger/cmd/collector/app/sanitizer"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/queue"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type spanProcessorTailSampling struct {
	queue              *queue.BoundedQueue
	queueResizeMu      sync.Mutex
	metrics            *SpanProcessorMetrics
	preProcessSpans    ProcessSpans
	filterSpan         FilterSpan             // filter is called before the sanitizer but after preProcessSpans
	sanitizer          sanitizer.SanitizeSpan // sanitizer is called before processSpan
	processSpan        ProcessSpan
	logger             *zap.Logger
	spanWriter         spanstore.Writer
	reportBusy         bool
	numWorkers         int
	collectorTags      map[string]string
	dynQueueSizeWarmup uint
	dynQueueSizeMemory uint
	bytesProcessed     *atomic.Uint64
	spansProcessed     *atomic.Uint64
	stopCh             chan struct{}
	idToTrace          sync.Map
	policyEvaluator    sampling.PolicyEvaluator
}

type queueElem struct {
	queuedTime time.Time
	TraceID    traceKey
}

// traceKey is defined since sync.Map requires a comparable type, isolating it on its own
// type to help track usage.
type traceKey string

// NewSpanProcessorTailSampling returns a SpanProcessor that preProcesses, filters, queues, sanitizes, and processes spans
func NewSpanProcessorTailSampling(
	spanWriter spanstore.Writer,
	opts ...Option,
) processor.SpanProcessor {
	sp := newSpanProcessorTailSampling(spanWriter, opts...)

	sp.queue.StartConsumers(sp.numWorkers, func(item interface{}) {
		value := item.(*queueElem)
		sp.processTraceIDFromQueue(value)
	})

	sp.background(1*time.Second, sp.updateGauges)

	if sp.dynQueueSizeMemory > 0 {
		sp.background(1*time.Minute, sp.updateQueueSize)
	}
	return sp
}

func newSpanProcessorTailSampling(spanWriter spanstore.Writer, opts ...Option) *spanProcessorTailSampling {
	options := Options.apply(opts...)
	handlerMetrics := NewSpanProcessorMetrics(
		options.serviceMetrics,
		options.hostMetrics,
		options.extraFormatTypes)
	droppedItemHandler := func(item interface{}) {
		handlerMetrics.SpansDropped.Inc(1)
	}
	boundedQueue := queue.NewBoundedQueue(options.queueSize, droppedItemHandler)

	sp := spanProcessorTailSampling{
		queue:              boundedQueue,
		metrics:            handlerMetrics,
		logger:             options.logger,
		preProcessSpans:    options.preProcessSpans,
		filterSpan:         options.spanFilter,
		sanitizer:          options.sanitizer,
		reportBusy:         options.reportBusy,
		numWorkers:         options.numWorkers,
		spanWriter:         spanWriter,
		collectorTags:      options.collectorTags,
		stopCh:             make(chan struct{}),
		dynQueueSizeMemory: options.dynQueueSizeMemory,
		dynQueueSizeWarmup: options.dynQueueSizeWarmup,
		bytesProcessed:     atomic.NewUint64(0),
		spansProcessed:     atomic.NewUint64(0),
		policyEvaluator:    options.policyEvaluator,
	}

	processSpanFuncs := []ProcessSpan{options.preSave, sp.saveSpan}
	if options.dynQueueSizeMemory > 0 {
		// add to processSpanFuncs
		options.logger.Info("Dynamically adjusting the queue size at runtime.",
			zap.Uint("memory-mib", options.dynQueueSizeMemory/1024/1024),
			zap.Uint("queue-size-warmup", options.dynQueueSizeWarmup))
		processSpanFuncs = append(processSpanFuncs, sp.countSpan)
	}
	sp.processSpan = ChainedProcessSpan(processSpanFuncs...)
	return &sp
}

func (sp *spanProcessorTailSampling) Close() error {
	close(sp.stopCh)
	sp.queue.Stop()

	return nil
}

func (sp *spanProcessorTailSampling) countSpan(span *model.Span) {
	sp.bytesProcessed.Add(uint64(span.Size()))
	sp.spansProcessed.Inc()
}

func (sp *spanProcessorTailSampling) saveSpan(span *model.Span) {
	fmt.Println(span.GetTags())
	if nil == span.Process {
		sp.logger.Error("process is empty for the span")
		sp.metrics.SavedErrBySvc.ReportServiceNameForSpan(span)
		return
	}
	startTime := time.Now()
	err := sp.spanWriter.WriteSpan(span)
	if err != nil {
		sp.logger.Error("Failed to save span", zap.Error(err))
		//sp.metrics.SavedErrBySvc.ReportServiceNameForSpan(span)
	} else {
		sp.logger.Debug("Span written to the storage by the collector",
			zap.Stringer("trace-id", span.TraceID), zap.Stringer("span-id", span.SpanID))
		//sp.metrics.SavedOkBySvc.ReportServiceNameForSpan(span)
	}
	sp.metrics.SaveLatency.Record(time.Since(startTime))
}

func (sp *spanProcessorTailSampling) ProcessSpans(mSpans []*model.Span, options processor.SpansOptions) ([]bool, error) {
	sp.preProcessSpans(mSpans)
	sp.metrics.BatchSize.Update(int64(len(mSpans)))

	idToSpans := make(map[traceKey][]*model.Span)
	for _, span := range mSpans {
		if len(span.TraceID.String()) != 16 {
			sp.logger.Warn("Span without valid TraceId", zap.String("TraceId", span.TraceID.String()))
			continue
		}
		traceKey := traceKey(span.TraceID.String())
		idToSpans[traceKey] = append(idToSpans[traceKey], span)
	}

	retMe := make([]bool, len(idToSpans))
	var newTraceIDs int64
	var i int64
	for id, spans := range idToSpans {
		fmt.Println(id)
		lenSpans := int64(len(spans))
		initialDecisions := make([]sampling.Decision, 1)
		for j := 0; j < 1; j++ {
			initialDecisions[j] = sampling.Pending
		}
		initialTraceData := &sampling.TraceData{
			Decisions:   initialDecisions,
			ArrivalTime: time.Now(),
			SpanCount:   lenSpans,
		}

		d, loaded := sp.idToTrace.LoadOrStore(traceKey(id), initialTraceData)
		actualData := d.(*sampling.TraceData)

		if loaded {
			//atomic.AddInt64(&actualData.SpanCount, lenSpans)
			retMe[i] = true
		} else {
			newTraceIDs++
			fmt.Println(newTraceIDs)
			ok := sp.enqueueTraceID(id)
			//atomic.AddUint64(&tsp.numTracesOnMap, 1)
			if !ok && sp.reportBusy {
				return nil, tchannel.ErrServerBusy
			}
			retMe[i] = ok
		}
		//retMe[i] = ok
		actualData.Lock()
		actualDecision := actualData.Decisions[0]
		// If decision is pending, we want to add the new spans still under the lock, so the decision doesn't happen
		// in between the transition from pending.
		if actualDecision == sampling.Pending {
			// Add the spans to the trace, but only once for all policy, otherwise same spans will
			// be duplicated in the final trace.
			traceTd := prepareTraceBatch(spans)
			actualData.ReceivedBatches = append(actualData.ReceivedBatches, traceTd)
		}
		actualData.Unlock()
		i++
	}
	/*
		for i, mSpan := range mSpans {
			ok := sp.enqueueSpan(mSpan, options.SpanFormat, options.InboundTransport)
			if !ok && sp.reportBusy {
				return nil, tchannel.ErrServerBusy
			}
			retMe[i] = ok
		}
	*/
	return retMe, nil
}

func prepareTraceBatch(spans []*model.Span) sampling.Spans {
	var spanBatch sampling.Spans
	spanBatch = sampling.Spans{
		Spans: spans,
	}
	return spanBatch
}

func (sp *spanProcessorTailSampling) processTraceIDFromQueue(item *queueElem) {
	//id = item.TraceID
	d, ok := sp.idToTrace.Load(item.TraceID)
	if !ok {
		//idNotFoundOnMapCount++
		return
	}
	trace := d.(*sampling.TraceData)
	trace.DecisionTime = time.Now()
	decision, err := sp.policyEvaluator.Evaluate(trace)
	trace.Decisions[0] = decision
	if err != nil {
		trace.Decisions[0] = sampling.NotSampled
	}

	switch trace.Decisions[0] {
	case sampling.Sampled:
		//decisionSampled++

		trace.Lock()
		traceBatches := trace.ReceivedBatches
		trace.Unlock()
		fmt.Println("here")
		for j := 0; j < len(traceBatches); j++ {
			spans := traceBatches[j]
			fmt.Println(len(spans.Spans))
			for i := 0; i < len(spans.Spans); i++ {
				fmt.Println(i)
				fmt.Println("here1")
				fmt.Println("here2")
				//sp.processSpan(sp.sanitizer(spans.Spans[i]))
				//sp.processSpan(spans.Spans[i])
				sp.saveSpan(spans.Spans[i])
			}
		}
	case sampling.NotSampled:
		//decisionNotSampled++
	}
}

func (sp *spanProcessorTailSampling) background(reportPeriod time.Duration, callback func()) {
	go func() {
		ticker := time.NewTicker(reportPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				callback()
			case <-sp.stopCh:
				return
			}
		}
	}()
}

func (sp *spanProcessorTailSampling) enqueueTraceID(traceID traceKey) bool {
	item := &queueElem{
		queuedTime: time.Now(),
		TraceID:    traceID,
	}
	return sp.queue.Produce(item)
}

func (sp *spanProcessorTailSampling) updateQueueSize() {
	if sp.dynQueueSizeWarmup == 0 {
		return
	}

	if sp.dynQueueSizeMemory == 0 {
		return
	}

	if sp.spansProcessed.Load() < uint64(sp.dynQueueSizeWarmup) {
		return
	}

	sp.queueResizeMu.Lock()
	defer sp.queueResizeMu.Unlock()

	// first, we get the average size of a span, by dividing the bytes processed by num of spans
	average := sp.bytesProcessed.Load() / sp.spansProcessed.Load()

	// finally, we divide the available memory by the average size of a span
	idealQueueSize := float64(sp.dynQueueSizeMemory / uint(average))

	// cap the queue size, just to be safe...
	if idealQueueSize > maxQueueSize {
		idealQueueSize = maxQueueSize
	}

	var diff float64
	current := float64(sp.queue.Capacity())
	if idealQueueSize > current {
		diff = idealQueueSize / current
	} else {
		diff = current / idealQueueSize
	}

	// resizing is a costly operation, we only perform it if we are at least n% apart from the desired value
	if diff > minRequiredChange {
		s := int(idealQueueSize)
		sp.logger.Info("Resizing the internal span queue", zap.Int("new-size", s), zap.Uint64("average-span-size-bytes", average))
		sp.queue.Resize(s)
	}
}

func (sp *spanProcessorTailSampling) updateGauges() {
	sp.metrics.SpansBytes.Update(int64(sp.bytesProcessed.Load()))
	sp.metrics.QueueLength.Update(int64(sp.queue.Size()))
	sp.metrics.QueueCapacity.Update(int64(sp.queue.Capacity()))
}
