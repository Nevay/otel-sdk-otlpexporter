<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Nevay\OTelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
use Nevay\OTelSDK\Otlp\Internal\SpanConverter;
use Nevay\OTelSDK\Trace\ReadableSpan;
use Nevay\OTelSDK\Trace\SpanExporter;
use Opentelemetry\Proto\Collector\Trace\V1\ExportTraceServiceRequest;
use Psr\Log\LoggerInterface;

/**
 * @implements OtlpStreamExporter<ReadableSpan, ExportTraceServiceRequest>
 */
final class OtlpStreamSpanExporter extends OtlpStreamExporter implements SpanExporter {

    public function __construct(WritableStream $stream, ?LoggerInterface $logger = null) {
        parent::__construct($stream, $logger);
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        $message = SpanConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceSpans()->count(),
        );
    }
}
