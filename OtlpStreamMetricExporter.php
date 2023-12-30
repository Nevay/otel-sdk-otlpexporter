<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Nevay\OtelSDK\Metrics\Data\Metric;
use Nevay\OtelSDK\Metrics\MetricExporter;
use Nevay\OtelSDK\Otlp\Internal\MetricConverter;
use Nevay\OtelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OtelSDK\Otlp\Internal\RequestPayload;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceRequest;
use Psr\Log\LoggerInterface;

/**
 * @implements OtlpStreamExporter<Metric, ExportMetricsServiceRequest>
 */
final class OtlpStreamMetricExporter extends OtlpStreamExporter implements MetricExporter {

    public function __construct(WritableStream $stream, ?LoggerInterface $logger = null) {
        parent::__construct($stream, $logger);
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        $message = MetricConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceMetrics()->count(),
        );
    }
}
