package jaegerB3Propagation

import (
	"net/url"
	"strconv"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"

	jaeger "github.com/uber/jaeger-client-go"
)

// Injector is responsible for injecting SpanContext instances in a manner suitable
// for propagation via a format-specific "carrier" object. Typically the
// injection will take place across an RPC boundary, but message queues and
// other IPC mechanisms are also reasonable places to use an Injector.
type Injector interface {
	// Inject takes `SpanContext` and injects it into `carrier`. The actual type
	// of `carrier` depends on the `format` passed to `Tracer.Inject()`.
	//
	// Implementations may return opentracing.ErrInvalidCarrier or any other
	// implementation-specific error if injection fails.
	Inject(ctx jaeger.SpanContext, carrier interface{}) error
}

// Extractor is responsible for extracting SpanContext instances from a
// format-specific "carrier" object. Typically the extraction will take place
// on the server side of an RPC boundary, but message queues and other IPC
// mechanisms are also reasonable places to use an Extractor.
type Extractor interface {
	// Extract decodes a SpanContext instance from the given `carrier`,
	// or (nil, opentracing.ErrSpanContextNotFound) if no context could
	// be found in the `carrier`.
	Extract(carrier interface{}) (jaeger.SpanContext, error)
}

type textMapPropagator struct {
	tracer      *opentracing.Tracer
	encodeValue func(string) string
	decodeValue func(string) string
}

func newTextMapPropagator(tracer *opentracing.Tracer) *textMapPropagator {
	return &textMapPropagator{
		tracer: tracer,
		encodeValue: func(val string) string {
			return val
		},
		decodeValue: func(val string) string {
			return val
		},
	}
}

func newHTTPHeaderPropagator(tracer *opentracing.Tracer) *textMapPropagator {
	return &textMapPropagator{
		tracer: tracer,
		encodeValue: func(val string) string {
			return url.QueryEscape(val)
		},
		decodeValue: func(val string) string {
			// ignore decoding errors, cannot do anything about them
			if v, err := url.QueryUnescape(val); err == nil {
				return v
			}
			return val
		},
	}
}

func (p *textMapPropagator) Inject(
	sc jaeger.SpanContext,
	abstractCarrier interface{},
) error {
	textMapWriter, ok := abstractCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}

	textMapWriter.Set("x-b3-traceid", p.encodeValue(strconv.FormatUint(sc.TraceID(), 16)))
	textMapWriter.Set("x-b3-parentspanid", p.encodeValue(strconv.FormatUint(sc.ParentID(), 16)))
	textMapWriter.Set("x-b3-spanid", p.encodeValue(strconv.FormatUint(sc.SpanID(), 16)))
	if sc.IsSampled() {
		textMapWriter.Set("x-b3-sampled", p.encodeValue("1"))
	}
	return nil
}

func (p *textMapPropagator) Extract(abstractCarrier interface{}) (jaeger.SpanContext, error) {
	textMapReader, ok := abstractCarrier.(opentracing.TextMapReader)
	if !ok {
		return jaeger.SpanContext{}, opentracing.ErrInvalidCarrier
	}
	var traceID uint64
	var spanID uint64
	var parentID uint64
	sampled := false
	err := textMapReader.ForeachKey(func(rawKey, value string) error {
		key := strings.ToLower(rawKey) // TODO not necessary for plain TextMap
		var err error
		if key == "x-b3-traceid" {
			traceID, err = strconv.ParseUint(p.decodeValue(value), 16, 64)
		} else if key == "x-b3-parentspanid" {
			parentID, err = strconv.ParseUint(p.decodeValue(value), 16, 64)
		} else if key == "x-b3-spanid" {
			spanID, err = strconv.ParseUint(p.decodeValue(value), 16, 64)
		} else if key == "x-b3-sampled" {
			sampled = true
		}
		return err
	})

	if err != nil {
		return jaeger.SpanContext{}, err
	}
	if traceID == 0 {
		return jaeger.SpanContext{}, opentracing.ErrSpanContextNotFound
	}
	return jaeger.NewSpanContext(traceID, spanID, parentID, sampled, nil), nil
}
