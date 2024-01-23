<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Nevay\OTelSDK\Logs\LogRecordExporter;
use Nevay\OTelSDK\Logs\ReadableLogRecord;
use Nevay\OTelSDK\Otlp\Internal\LogRecordConverter;
use Nevay\OTelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceRequest;
use Psr\Log\LoggerInterface;

/**
 * @implements OtlpStreamExporter<ReadableLogRecord, ExportLogsServiceRequest>
 */
final class OtlpStreamLogRecordExporter extends OtlpStreamExporter implements LogRecordExporter {

    public function __construct(WritableStream $stream, ?LoggerInterface $logger = null) {
        parent::__construct($stream, $logger);
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        $message = LogRecordConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceLogs()->count(),
        );
    }
}
