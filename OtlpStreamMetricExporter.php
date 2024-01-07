<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Nevay\OtelSDK\Metrics\Aggregation;
use Nevay\OtelSDK\Metrics\AggregationResolver;
use Nevay\OtelSDK\Metrics\AggregationResolvers;
use Nevay\OtelSDK\Metrics\CardinalityLimitResolver;
use Nevay\OtelSDK\Metrics\CardinalityLimitResolvers;
use Nevay\OtelSDK\Metrics\Data\Descriptor;
use Nevay\OtelSDK\Metrics\Data\Metric;
use Nevay\OtelSDK\Metrics\Data\Temporality;
use Nevay\OtelSDK\Metrics\InstrumentType;
use Nevay\OtelSDK\Metrics\MetricExporter;
use Nevay\OtelSDK\Metrics\TemporalityResolver;
use Nevay\OtelSDK\Metrics\TemporalityResolvers;
use Nevay\OtelSDK\Otlp\Internal\MetricConverter;
use Nevay\OtelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OtelSDK\Otlp\Internal\RequestPayload;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceRequest;
use Psr\Log\LoggerInterface;

/**
 * @implements OtlpStreamExporter<Metric, ExportMetricsServiceRequest>
 */
final class OtlpStreamMetricExporter extends OtlpStreamExporter implements MetricExporter {

    public function __construct(
        WritableStream $stream,
        private readonly TemporalityResolver $temporalityResolver = TemporalityResolvers::LowMemory,
        private readonly AggregationResolver $aggregationResolver = AggregationResolvers::Default,
        private readonly CardinalityLimitResolver $cardinalityLimitResolver = CardinalityLimitResolvers::Default,
        ?LoggerInterface $logger = null,
    ) {
        parent::__construct($stream, $logger);
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        $message = MetricConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceMetrics()->count(),
        );
    }

    public function resolveTemporality(Descriptor $descriptor): ?Temporality {
        return $this->temporalityResolver->resolveTemporality($descriptor);
    }

    public function resolveAggregation(InstrumentType $instrumentType, array $advisory = []): ?Aggregation {
        return $this->aggregationResolver->resolveAggregation($instrumentType, $advisory);
    }

    public function resolveCardinalityLimit(InstrumentType $instrumentType): ?int {
        return $this->cardinalityLimitResolver->resolveCardinalityLimit($instrumentType);
    }
}
