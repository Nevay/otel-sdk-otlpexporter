<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

use Nevay\OTelSDK\Common\InstrumentationScope;
use Nevay\OTelSDK\Common\Resource;
use Nevay\OTelSDK\Logs\ReadableLogRecord;
use Nevay\OTelSDK\Otlp\ProtobufFormat;
use Opentelemetry\Proto;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceRequest;
use function spl_object_id;

/**
 * @internal
 */
final class LogRecordConverter {

    /**
     * @param iterable<ReadableLogRecord> $batch
     */
    public static function convert(iterable $batch, ProtobufFormat $format): ExportLogsServiceRequest {
        $pExportLogsServiceRequest = new ExportLogsServiceRequest();

        $resourceLogs = [];
        $scopeLogs = [];
        foreach ($batch as $logRecord) {
            $resource = $logRecord->getResource();
            $instrumentationScope = $logRecord->getInstrumentationScope();

            $resourceId = spl_object_id($resource);
            $instrumentationScopeId = Converter::instrumentationScopeId($instrumentationScope);

            $pResourceLogs = $resourceLogs[$resourceId]
                ??= $pExportLogsServiceRequest->getResourceLogs()[]
                = self::convertResourceLogs($resource);
            $pScopeLogs = $scopeLogs[$resourceId][$instrumentationScopeId]
                ??= $pResourceLogs->getScopeLogs()[]
                = self::convertScopeLogs($instrumentationScope);

            $pScopeLogs->getLogRecords()[] = self::convertLogRecord($logRecord, $format);
        }

        return $pExportLogsServiceRequest;
    }

    private static function convertResourceLogs(Resource $resource): Proto\Logs\V1\ResourceLogs {
        $pResourceMetrics = new Proto\Logs\V1\ResourceLogs();
        $pResourceMetrics->setResource(Converter::convertResource($resource));
        $pResourceMetrics->setSchemaUrl((string) $resource->schemaUrl);

        return $pResourceMetrics;
    }

    private static function convertScopeLogs(InstrumentationScope $instrumentationScope): Proto\Logs\V1\ScopeLogs {
        $pScopeMetrics = new Proto\Logs\V1\ScopeLogs();
        $pScopeMetrics->setScope(Converter::convertInstrumentationScope($instrumentationScope));
        $pScopeMetrics->setSchemaUrl((string) $instrumentationScope->schemaUrl);

        return $pScopeMetrics;
    }

    private static function convertLogRecord(ReadableLogRecord $logRecord, ProtobufFormat $format): Proto\Logs\V1\LogRecord {
        $pLogRecord = new Proto\Logs\V1\LogRecord();
        $pLogRecord->setTimeUnixNano($logRecord->getTimestamp() ?? 0);
        $pLogRecord->setObservedTimeUnixNano($logRecord->getObservedTimestamp() ?? 0);
        $pLogRecord->setSeverityNumber($logRecord->getSeverityNumber() ?? 0);
        $pLogRecord->setSeverityText($logRecord->getSeverityText() ?? '');
        $pLogRecord->setBody(Converter::convertAnyValue($logRecord->getBody()));
        foreach ($logRecord->getAttributes() as $key => $value) {
            $pLogRecord->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($key)
                ->setValue(Converter::convertAnyValue($value));
        }
        $pLogRecord->setDroppedAttributesCount($logRecord->getAttributes()->getDroppedAttributesCount());
        if ($spanContext = $logRecord->getSpanContext()) {
            $pLogRecord->setFlags($spanContext->getTraceFlags());
            $pLogRecord->setTraceId(Converter::traceId($spanContext, $format));
            $pLogRecord->setSpanId(Converter::spanId($spanContext, $format));
        }

        return $pLogRecord;
    }
}
