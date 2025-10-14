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

        $version = InstalledVersions::getPrettyVersion('tbachert/otel-sdk-otlpexporter');
        $meter = $meterProvider->getMeter('com.tobiasbachert.otel.sdk.otlpexporter', $version, 'https://opentelemetry.io/schemas/1.36.0');

        $inflight = $meter->createUpDownCounter(
            'otel.sdk.exporter.log.inflight',
            '{log_record}',
            'The number of log records which were passed to the exporter, but that have not been exported yet (neither successful, nor failed)',
        );
        $exported = $meter->createCounter(
            'otel.sdk.exporter.log.exported',
            '{log_record}',
            'The number of log records for which the export has finished, either successful or failed',
        );
        $duration = $meter->createHistogram(
            'otel.sdk.exporter.operation.duration',
            's',
            'The duration of exporting a batch of telemetry records',
            advisory: ['ExplicitBucketBoundaries' => []],
        );

        parent::__construct(
            $stream,
            $logger,
            $inflight,
            $exported,
            $duration,
            $type,
            $name,
        );
    }

    protected function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload {
        return new RequestPayload(
            LogRecordConverter::convert($batch, $format, $count),
            $count,
        );
    }
}
