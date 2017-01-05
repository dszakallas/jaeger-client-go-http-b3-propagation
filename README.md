# jaeger-client-go-http-b3-propagation
OpenZipkin B3 http header propagation for jaeger-client-go

Adds support for reading Zipkin B3 Propagation HTTP headers

```

// ...

func main() {

	// ...

	zipkinPropagator := jaegerB3Propagation.NewZipkinB3HTTPHeaderPropagator()

	injector := jaeger.TracerOptions.Injector(opentracing.HTTPHeaders, zipkinPropagator)
	extractor := jaeger.TracerOptions.Extractor(opentracing.HTTPHeaders, zipkinPropagator)

	// create Jaeger tracer
	tracer, closer := jaeger.NewTracer(
		"myService",
		mySampler, // as usual
		myReporter // as usual
		injector,
		extractor,
	)
```
