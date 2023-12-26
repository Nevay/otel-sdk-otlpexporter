<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\Cancellation;
use Amp\Future;
use Amp\Http\Client\HttpClient;
use JetBrains\PhpStorm\ExpectedValues;
use Nevay\OtelSDK\Metrics\MetricExporter;
use Nevay\OtelSDK\Otlp\Internal\MetricConverter;
use Nevay\OtelSDK\Otlp\Internal\OtlpHttpExporter;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceResponse;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;

final class OtlpHttpMetricExporter implements MetricExporter {

    private readonly OtlpHttpExporter $exporter;

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
        $this->exporter = new OtlpHttpExporter(
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

    public function export(iterable $batch, ?Cancellation $cancellation = null): Future {
        return $this->exporter->export($batch, $cancellation, MetricConverter::convert(...), self::processResponse(...), ExportMetricsServiceResponse::class);
    }

    public function shutdown(?Cancellation $cancellation = null): bool {
        return $this->exporter->shutdown($cancellation);
    }

    public function forceFlush(?Cancellation $cancellation = null): bool {
        return $this->exporter->forceFlush($cancellation);
    }

    private static function processResponse(ExportMetricsServiceResponse $message, ?LoggerInterface $logger): bool {
        $partialSuccess = $message->getPartialSuccess();
        if ($partialSuccess?->getRejectedDataPoints()) {
            $logger?->error('Export partial success', [
                'rejected_data_points' => $partialSuccess->getRejectedDataPoints(),
                'error_message' => $partialSuccess->getErrorMessage(),
            ]);
            return false;
        }

        if ($partialSuccess?->getErrorMessage()) {
            $logger?->warning('Export success with warnings/suggestions', [
                'error_message' => $partialSuccess->getErrorMessage(),
            ]);
        }

        return true;
    }
}
