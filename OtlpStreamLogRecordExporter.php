<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Composer\InstalledVersions;
use Nevay\OTelSDK\Logs\LogRecordExporter;
use Nevay\OTelSDK\Logs\ReadableLogRecord;
use Nevay\OTelSDK\Otlp\Internal\LogRecordConverter;
use Nevay\OTelSDK\Otlp\Internal\OtlpStreamExporter;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
use OpenTelemetry\API\Metrics\MeterProviderInterface;
use OpenTelemetry\API\Metrics\Noop\NoopMeterProvider;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceRequest;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * @implements OtlpStreamExporter<ReadableLogRecord, ExportLogsServiceRequest>
 */
final class OtlpStreamLogRecordExporter extends OtlpStreamExporter implements LogRecordExporter {

    private static int $instanceCounter = -1;

    public function __construct(
        WritableStream $stream,
        MeterProviderInterface $meterProvider = new NoopMeterProvider(),
        LoggerInterface $logger = new NullLogger(),
    ) {
        $type = 'otlp_stream_logrecord_exporter';
        $name ??= $type . '/' . ++self::$instanceCounter;

        $version = InstalledVersions::getVersionRanges('tbachert/otel-sdk-otlpexporter');
        $meter = $meterProvider->getMeter('com.tobiasbachert.otel.sdk.otlpexporter', $version);

        $inflight = $meter->createUpDownCounter(
            'otel.sdk.log.exporter.logrecords_inflight',
            '{metric}',
            'The number of log records which were passed to the exporter, but that have not been exported yet (neither successful, nor failed)',
        );
        $exported = $meter->createCounter(
            'otel.sdk.log.exporter.logrecords_exported',
            '{metric}',
            'The number of log records for which the export has finished, either successful or failed',
        );

        parent::__construct(
            $stream,
            $logger,
            $inflight,
            $exported,
            $type,
            $name,
        );
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        $message = LogRecordConverter::convert($batch, $format);

        return new RequestPayload(
            $message,
            $message->getResourceLogs()->count(),
        );
    }
}
