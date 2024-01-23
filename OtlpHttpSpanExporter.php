<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\Http\Client\HttpClient;
use Google\Protobuf\Internal\Message;
use JetBrains\PhpStorm\ExpectedValues;
use Nevay\OTelSDK\Otlp\Internal\OtlpHttpExporter;
use Nevay\OTelSDK\Otlp\Internal\PartialSuccess;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
use Nevay\OTelSDK\Otlp\Internal\SpanConverter;
use Nevay\OTelSDK\Trace\ReadableSpan;
use Nevay\OTelSDK\Trace\SpanExporter;
use Opentelemetry\Proto\Collector\Trace\V1\ExportTraceServiceRequest;
use Opentelemetry\Proto\Collector\Trace\V1\ExportTraceServiceResponse;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;

/**
 * @implements OtlpHttpExporter<ReadableSpan, ExportTraceServiceRequest, ExportTraceServiceResponse>
 */
final class OtlpHttpSpanExporter extends OtlpHttpExporter implements SpanExporter {

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
            ExportTraceServiceResponse::class,
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
        $message = SpanConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceSpans()->count(),
        );
    }

    protected function convertResponse(Message $message): ?PartialSuccess {
        if (!$partialSuccess = $message->getPartialSuccess()) {
            return null;
        }

        return new PartialSuccess(
            $partialSuccess->getErrorMessage(),
            $partialSuccess->getRejectedSpans(),
        );
    }
}
