<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\Http\Client\HttpClient;
use Google\Protobuf\Internal\Message;
use JetBrains\PhpStorm\ExpectedValues;
use Nevay\OtelSDK\Metrics\Data\Metric;
use Nevay\OtelSDK\Metrics\MetricExporter;
use Nevay\OtelSDK\Otlp\Internal\MetricConverter;
use Nevay\OtelSDK\Otlp\Internal\OtlpHttpExporter;
use Nevay\OtelSDK\Otlp\Internal\PartialSuccess;
use Nevay\OtelSDK\Otlp\Internal\RequestPayload;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceRequest;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceResponse;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;

/**
 * @implements OtlpHttpExporter<Metric, ExportMetricsServiceRequest, ExportMetricsServiceResponse>
 */
final class OtlpHttpMetricExporter extends OtlpHttpExporter implements MetricExporter {

    public function __construct(
        HttpClient $client,
        UriInterface $endpoint,
        ProtobufFormat $format = ProtobufFormat::PROTOBUF,
        #[ExpectedValues(values: ['gzip', null])]
        ?string $compression = null,
        array $headers = [],
        float $timeout = 10.,
        int $retryDelay = 5000,
        int $maxRetries = 5,
        ?LoggerInterface $logger = null,
    ) {
        parent::__construct(
            ExportMetricsServiceResponse::class,
            $client,
            $endpoint,
            $format,
            $compression,
            $headers,
            $timeout,
            $retryDelay,
            $maxRetries,
            $logger,
        );
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        $message = MetricConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceMetrics()->count(),
        );
    }

    protected function convertResponse(Message $message): ?PartialSuccess {
        if (!$partialSuccess = $message->getPartialSuccess()) {
            return null;
        }

        return new PartialSuccess(
            $partialSuccess->getErrorMessage(),
            $partialSuccess->getRejectedDataPoints(),
        );
    }
}
