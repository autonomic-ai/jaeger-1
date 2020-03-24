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
	"io"
	"testing"
	"time"

	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/metricstest"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/collector/app/handler"
	"github.com/jaegertracing/jaeger/cmd/collector/app/sampling"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
)

var (
	_ io.Closer = (*fakeSpanWriter)(nil)
	_ io.Closer = (*spanProcessor)(nil)
)

func TestTailSampling(t *testing.T) {
	logger := zap.NewNop()
	mb := metricstest.NewFactory(time.Hour)
	serviceMetrics := mb.Namespace(metrics.NSOptions{Name: "service", Tags: nil})
	hostMetrics := mb.Namespace(metrics.NSOptions{Name: "host", Tags: nil})

	values := []string{"404"}
	sp := NewSpanProcessorTailSampling(
		&fakeSpanWriter{},
		Options.ServiceMetrics(serviceMetrics),
		Options.HostMetrics(hostMetrics),
		Options.Logger(logger),
		Options.QueueSize(100),
		Options.BlockingSubmit(false),
		Options.ReportBusy(false),
		Options.SpanFilter(isSpanAllowed),
		Options.PolicyEvaluator(sampling.NewStringAttributeFilter(logger, "http", values)),
	)
	serviceName := "test"
	rootSpan := true
	debug := true
	tags := []*jaeger.Tag{
		makeJaegerTag("http", "404"),
		makeJaegerTag("http", "401"),
	}

	span, process := makeJaegerSpanWithTags(serviceName, rootSpan, debug, 42, tags)
	jHandler := handler.NewJaegerSpanHandler(logger, sp)
	jHandler.SubmitBatches([]*jaeger.Batch{
		{
			Spans: []*jaeger.Span{
				span,
				span,
			},
			Process: process,
		},
	}, handler.SubmitBatchOptions{})
}

func makeJaegerTag(key string, value string) *jaeger.Tag {
	return &jaeger.Tag{
		Key:  key,
		VStr: &value,
	}
}
func makeJaegerSpanWithTags(service string, rootSpan bool, debugEnabled bool, traceID int64, tags []*jaeger.Tag) (*jaeger.Span, *jaeger.Process) {
	flags := int32(0)
	if debugEnabled {
		flags = 2
	}
	parentID := int64(0)
	if !rootSpan {
		parentID = int64(1)
	}
	return &jaeger.Span{
			OperationName: "jaeger",
			Flags:         flags,
			ParentSpanId:  parentID,
			TraceIdLow:    traceID,
			Tags:          tags,
		}, &jaeger.Process{
			ServiceName: service,
		}
}
