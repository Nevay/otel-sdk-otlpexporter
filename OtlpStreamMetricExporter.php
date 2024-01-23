<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Nevay\OTelSDK\Metrics\Aggregation;
use Nevay\OTelSDK\Metrics\AggregationResolver;
use Nevay\OTelSDK\Metrics\AggregationResolvers;
use Nevay\OTelSDK\Metrics\CardinalityLimitResolver;
use Nevay\OTelSDK\Metrics\CardinalityLimitResolvers;
use Nevay\OTelSDK\Metrics\Data\Descriptor;
use Nevay\OTelSDK\Metrics\Data\Metric;
use Nevay\OTelSDK\Metrics\Data\Temporality;
use Nevay\OTelSDK\Metrics\InstrumentType;
use Nevay\OTelSDK\Metrics\MetricExporter;
use Nevay\OTelSDK\Metrics\TemporalityResolver;
use Nevay\OTelSDK\Metrics\TemporalityResolvers;
use Nevay\OTelSDK\Otlp\Internal\MetricConverter;
use Nevay\OTelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
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
