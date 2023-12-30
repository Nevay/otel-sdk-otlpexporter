<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Nevay\OtelSDK\Logs\LogRecordExporter;
use Nevay\OtelSDK\Logs\ReadableLogRecord;
use Nevay\OtelSDK\Otlp\Internal\LogRecordConverter;
use Nevay\OtelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OtelSDK\Otlp\Internal\RequestPayload;
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
