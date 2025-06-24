<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Composer\InstalledVersions;
use Nevay\OTelSDK\Metrics\Aggregation;
use Nevay\OTelSDK\Metrics\Aggregation\DefaultAggregation;
use Nevay\OTelSDK\Metrics\Data\Metric;
use Nevay\OTelSDK\Metrics\Data\Temporality;
use Nevay\OTelSDK\Metrics\InstrumentType;
use Nevay\OTelSDK\Metrics\MetricExporter;
use Nevay\OTelSDK\Metrics\TemporalityResolver;
use Nevay\OTelSDK\Otlp\Internal\MetricConverter;
use Nevay\OTelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
use OpenTelemetry\API\Metrics\MeterProviderInterface;
use OpenTelemetry\API\Metrics\Noop\NoopMeterProvider;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceRequest;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * @implements OtlpStreamExporter<Metric, ExportMetricsServiceRequest>
 */
final class OtlpStreamMetricExporter extends OtlpStreamExporter implements MetricExporter {

    private static int $instanceCounter = -1;

    public function __construct(
        WritableStream $stream,
        private readonly TemporalityResolver $temporalityResolver = OltpTemporality::Cumulative,
        private readonly Aggregation $aggregation = new DefaultAggregation(),
        MeterProviderInterface $meterProvider = new NoopMeterProvider(),
        LoggerInterface $logger = new NullLogger(),
    ) {
        $type = 'otlp_stream_metric_exporter';
        $name ??= $type . '/' . ++self::$instanceCounter;

        $version = InstalledVersions::getVersionRanges('tbachert/otel-sdk-otlpexporter');
        $meter = $meterProvider->getMeter('com.tobiasbachert.otel.sdk.otlpexporter', $version, 'https://opentelemetry.io/schemas/1.34.0');

        $inflight = $meter->createUpDownCounter(
            'otel.sdk.exporter.metric_data_point.inflight',
            '{data_point}',
            'The number of metrics which were passed to the exporter, but that have not been exported yet (neither successful, nor failed)',
        );
        $exported = $meter->createCounter(
            'otel.sdk.exporter.metric_data_point.exported',
            '{data_point}',
            'The number of metrics for which the export has finished, either successful or failed',
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
            MetricConverter::convert($batch, $format, $count),
            $count,
        );
    }

    public function resolveTemporality(InstrumentType $instrumentType, Temporality $preferredTemporality): Temporality {
        return $this->temporalityResolver->resolveTemporality($instrumentType, $preferredTemporality);
    }

    public function resolveAggregation(InstrumentType $instrumentType): Aggregation {
        return $this->aggregation;
    }
}
