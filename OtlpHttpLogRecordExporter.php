<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\Http\Client\HttpClient;
use Google\Protobuf\Internal\Message;
use JetBrains\PhpStorm\ExpectedValues;
use Nevay\OtelSDK\Logs\LogRecordExporter;
use Nevay\OtelSDK\Logs\ReadableLogRecord;
use Nevay\OtelSDK\Otlp\Internal\LogRecordConverter;
use Nevay\OtelSDK\Otlp\Internal\OtlpHttpExporter;
use Nevay\OtelSDK\Otlp\Internal\PartialSuccess;
use Nevay\OtelSDK\Otlp\Internal\RequestPayload;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceRequest;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceResponse;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;

/**
 * @implements OtlpHttpExporter<ReadableLogRecord, ExportLogsServiceRequest, ExportLogsServiceResponse>
 */
final class OtlpHttpLogRecordExporter extends OtlpHttpExporter implements LogRecordExporter {

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
            ExportLogsServiceResponse::class,
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
        $message = LogRecordConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceLogs()->count(),
        );
    }

    protected function convertResponse(Message $message): ?PartialSuccess {
        if (!$partialSuccess = $message->getPartialSuccess()) {
            return null;
        }

        return new PartialSuccess(
            $partialSuccess->getErrorMessage(),
            $partialSuccess->getRejectedLogRecords(),
        );
    }
}
