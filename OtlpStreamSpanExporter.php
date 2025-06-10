<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Composer\InstalledVersions;
use Nevay\OTelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
use Nevay\OTelSDK\Otlp\Internal\SpanConverter;
use Nevay\OTelSDK\Trace\ReadableSpan;
use Nevay\OTelSDK\Trace\SpanExporter;
use OpenTelemetry\API\Metrics\MeterProviderInterface;
use OpenTelemetry\API\Metrics\Noop\NoopMeterProvider;
use Opentelemetry\Proto\Collector\Trace\V1\ExportTraceServiceRequest;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * @implements OtlpStreamExporter<ReadableSpan, ExportTraceServiceRequest>
 */
final class OtlpStreamSpanExporter extends OtlpStreamExporter implements SpanExporter {

    private static int $instanceCounter = -1;

    public function __construct(
        WritableStream $stream,
        MeterProviderInterface $meterProvider = new NoopMeterProvider(),
        LoggerInterface $logger = new NullLogger(),
    ) {
        $type = 'otlp_stream_span_exporter';
        $name ??= $type . '/' . ++self::$instanceCounter;

        $version = InstalledVersions::getVersionRanges('tbachert/otel-sdk-otlpexporter');
        $meter = $meterProvider->getMeter('com.tobiasbachert.otel.sdk.otlpexporter', $version, 'https://opentelemetry.io/schemas/1.34.0');

        $inflight = $meter->createUpDownCounter(
            'otel.sdk.exporter.span.inflight',
            '{span}',
            'The number of spans which were passed to the exporter, but that have not been exported yet (neither successful, nor failed)',
        );
        $exported = $meter->createCounter(
            'otel.sdk.exporter.span.exported',
            '{span}',
            'The number of spans for which the export has finished, either successful or failed',
        );
        $duration = $meter->createHistogram(
            'otel.sdk.exporter.operation.duration',
            's',
            'The duration of exporting a batch of telemetry records',
            advisory: ['ExplicitBucketBoundaries' => []],
        );

        parent::__construct(
            $stream,
            $logger,
            $inflight,
            $exported,
            $duration,
            $type,
            $name,
        );
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        return new RequestPayload(
            SpanConverter::convert($batch, $format, $count),
            $count,
        );
    }
}
