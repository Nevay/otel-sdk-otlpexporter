<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Nevay\OtelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OtelSDK\Otlp\Internal\RequestPayload;
use Nevay\OtelSDK\Otlp\Internal\SpanConverter;
use Nevay\OtelSDK\Trace\ReadableSpan;
use Nevay\OtelSDK\Trace\SpanExporter;
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
