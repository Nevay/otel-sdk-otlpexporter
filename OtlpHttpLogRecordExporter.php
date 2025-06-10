<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Amp\Http\Client\HttpClient;
use Composer\InstalledVersions;
use Google\Protobuf\Internal\Message;
use JetBrains\PhpStorm\ExpectedValues;
use Nevay\OTelSDK\Logs\LogRecordExporter;
use Nevay\OTelSDK\Logs\ReadableLogRecord;
use Nevay\OTelSDK\Otlp\Internal\LogRecordConverter;
use Nevay\OTelSDK\Otlp\Internal\OtlpHttpExporter;
use Nevay\OTelSDK\Otlp\Internal\PartialSuccess;
use Nevay\OTelSDK\Otlp\Internal\RequestPayload;
use OpenTelemetry\API\Metrics\MeterProviderInterface;
use OpenTelemetry\API\Metrics\Noop\NoopMeterProvider;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceRequest;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceResponse;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * @implements OtlpHttpExporter<ReadableLogRecord, ExportLogsServiceRequest, ExportLogsServiceResponse>
 */
final class OtlpHttpLogRecordExporter extends OtlpHttpExporter implements LogRecordExporter {

    private static int $instanceCounter = -1;

    public function __construct(
        HttpClient $client,
        UriInterface $endpoint,
        ProtobufFormat $format = ProtobufFormat::Protobuf,
        #[ExpectedValues(values: ['gzip', null])]
        ?string $compression = null,
        array $headers = [],
        float $timeout = 10.,
        int $retryDelay = 5000,
        int $maxRetries = 5,
        MeterProviderInterface $meterProvider = new NoopMeterProvider(),
        LoggerInterface $logger = new NullLogger(),
        ?string $name = null,
    ) {
        $type = match ($format) {
            ProtobufFormat::Protobuf => 'otlp_http_logrecord_exporter',
            ProtobufFormat::Json => 'otlp_http_json_logrecord_exporter',
        };
        $name ??= $type . '/' . ++self::$instanceCounter;

        $version = InstalledVersions::getVersionRanges('tbachert/otel-sdk-otlpexporter');
        $meter = $meterProvider->getMeter('com.tobiasbachert.otel.sdk.otlpexporter', $version, 'https://opentelemetry.io/schemas/1.34.0');

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
