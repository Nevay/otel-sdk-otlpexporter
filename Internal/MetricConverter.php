<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp\Internal;

use Nevay\OtelSDK\Common\InstrumentationScope;
use Nevay\OtelSDK\Common\Resource;
use Nevay\OtelSDK\Metrics\Data\Exemplar;
use Nevay\OtelSDK\Metrics\Data\Gauge;
use Nevay\OtelSDK\Metrics\Data\Histogram;
use Nevay\OtelSDK\Metrics\Data\HistogramDataPoint;
use Nevay\OtelSDK\Metrics\Data\Metric;
use Nevay\OtelSDK\Metrics\Data\NumberDataPoint;
use Nevay\OtelSDK\Metrics\Data\Sum;
use Nevay\OtelSDK\Metrics\Data\Temporality;
use Nevay\OtelSDK\Otlp\ProtobufFormat;
use Opentelemetry\Proto;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceRequest;
use function spl_object_id;

/**
 * @internal
 */
final class MetricConverter {

    /**
     * @param iterable<Metric> $batch
     */
    public static function convert(iterable $batch, ProtobufFormat $format): ?ExportMetricsServiceRequest {
        $pExportMetricsServiceRequest = new ExportMetricsServiceRequest();

        $resourceMetrics = [];
        $scopeMetrics = [];
        foreach ($batch as $metric) {
            if (!$metric->data->dataPoints) {
                continue;
            }

            $resource = $metric->descriptor->resource;
            $instrumentationScope = $metric->descriptor->instrumentationScope;

            $resourceId = spl_object_id($resource);
            $instrumentationScopeId = Converter::instrumentationScopeId($instrumentationScope);

            $pResourceMetrics = $resourceMetrics[$resourceId]
                ??= $pExportMetricsServiceRequest->getResourceMetrics()[]
                = self::convertResourceMetrics($resource);
            $pScopeMetrics = $scopeMetrics[$resourceId][$instrumentationScopeId]
                ??= $pResourceMetrics->getScopeMetrics()[]
                = self::convertScopeMetrics($instrumentationScope);

            $pScopeMetrics->getMetrics()[] = self::convertMetric($metric, $format);
        }

        if (!$resourceMetrics) {
            return null;
        }

        return $pExportMetricsServiceRequest;
    }

    private static function convertResourceMetrics(Resource $resource): Proto\Metrics\V1\ResourceMetrics {
        $pResourceMetrics = new Proto\Metrics\V1\ResourceMetrics();
        $pResourceMetrics->setResource(Converter::convertResource($resource));
        $pResourceMetrics->setSchemaUrl((string) $resource->schemaUrl);

        return $pResourceMetrics;
    }

    private static function convertScopeMetrics(InstrumentationScope $instrumentationScope): Proto\Metrics\V1\ScopeMetrics {
        $pScopeMetrics = new Proto\Metrics\V1\ScopeMetrics();
        $pScopeMetrics->setScope(Converter::convertInstrumentationScope($instrumentationScope));
        $pScopeMetrics->setSchemaUrl((string) $instrumentationScope->schemaUrl);

        return $pScopeMetrics;
    }

    private static function convertMetric(Metric $metric, ProtobufFormat $format): Proto\Metrics\V1\Metric {
        $pMetric = new Proto\Metrics\V1\Metric();
        $pMetric->setName($metric->descriptor->name);
        $pMetric->setUnit((string) $metric->descriptor->unit);
        $pMetric->setDescription((string) $metric->descriptor->description);

        $data = $metric->data;
        if ($data instanceof Gauge) {
            $pMetric->setGauge(self::convertGauge($data, $format));
        }
        if ($data instanceof Histogram) {
            $pMetric->setHistogram(self::convertHistogram($data, $format));
        }
        if ($data instanceof Sum) {
            $pMetric->setSum(self::convertSum($data, $format));
        }

        return $pMetric;
    }

    private static function convertTemporality(Temporality $temporality): int {
        return match ($temporality) {
            Temporality::Delta => Proto\Metrics\V1\AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA,
            Temporality::Cumulative => Proto\Metrics\V1\AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE
        };
    }

    private static function convertGauge(Gauge $gauge, ProtobufFormat $format): Proto\Metrics\V1\Gauge {
        $pGauge = new Proto\Metrics\V1\Gauge();
        foreach ($gauge->dataPoints as $dataPoint) {
            $pGauge->getDataPoints()[] = self::convertNumberDataPoint($dataPoint, $format);
        }

        return $pGauge;
    }

    private static function convertHistogram(Histogram $histogram, ProtobufFormat $format): Proto\Metrics\V1\Histogram {
        $pHistogram = new Proto\Metrics\V1\Histogram();
        foreach ($histogram->dataPoints as $dataPoint) {
            $pHistogram->getDataPoints()[] = self::convertHistogramDataPoint($dataPoint, $format);
        }
        $pHistogram->setAggregationTemporality(self::convertTemporality($histogram->temporality));

        return $pHistogram;
    }

    private static function convertSum(Sum $sum, ProtobufFormat $format): Proto\Metrics\V1\Sum {
        $pSum = new Proto\Metrics\V1\Sum();
        foreach ($sum->dataPoints as $dataPoint) {
            $pSum->getDataPoints()[] = self::convertNumberDataPoint($dataPoint, $format);
        }
        $pSum->setAggregationTemporality(self::convertTemporality($sum->temporality));
        $pSum->setIsMonotonic($sum->monotonic);

        return $pSum;
    }

    private static function convertNumberDataPoint(NumberDataPoint $dataPoint, ProtobufFormat $format): Proto\Metrics\V1\NumberDataPoint {
        $pNumberDataPoint = new Proto\Metrics\V1\NumberDataPoint();
        foreach ($dataPoint->attributes as $key => $value) {
            $pNumberDataPoint->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($key)
                ->setValue(Converter::convertAnyValue($value));
        }
        $pNumberDataPoint->setStartTimeUnixNano($dataPoint->startTimestamp);
        $pNumberDataPoint->setTimeUnixNano($dataPoint->timestamp);
        if (is_int($dataPoint->value)) {
            $pNumberDataPoint->setAsInt($dataPoint->value);
        }
        if (is_float($dataPoint->value)) {
            $pNumberDataPoint->setAsDouble($dataPoint->value);
        }
        foreach ($dataPoint->exemplars as $exemplar) {
            $pNumberDataPoint->getExemplars()[] = self::convertExemplar($exemplar, $format);
        }

        return $pNumberDataPoint;
    }

    private static function convertHistogramDataPoint(HistogramDataPoint $dataPoint, ProtobufFormat $format): Proto\Metrics\V1\HistogramDataPoint {
        $pHistogramDataPoint = new Proto\Metrics\V1\HistogramDataPoint();
        foreach ($dataPoint->attributes as $key => $value) {
            $pHistogramDataPoint->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($key)
                ->setValue(Converter::convertAnyValue($value));
        }
        $pHistogramDataPoint->setStartTimeUnixNano($dataPoint->startTimestamp);
        $pHistogramDataPoint->setTimeUnixNano($dataPoint->timestamp);
        $pHistogramDataPoint->setCount($dataPoint->count);
        $pHistogramDataPoint->setSum($dataPoint->sum);
        $pHistogramDataPoint->setMin($dataPoint->min);
        $pHistogramDataPoint->setMax($dataPoint->max);
        $pHistogramDataPoint->setBucketCounts($dataPoint->bucketCounts);
        $pHistogramDataPoint->setExplicitBounds($dataPoint->explicitBounds);
        foreach ($dataPoint->exemplars as $exemplar) {
            $pHistogramDataPoint->getExemplars()[] = self::convertExemplar($exemplar, $format);
        }

        return $pHistogramDataPoint;
    }

    private static function convertExemplar(Exemplar $exemplar, ProtobufFormat $format): Proto\Metrics\V1\Exemplar {
        $pExemplar = new Proto\Metrics\V1\Exemplar();
        foreach ($exemplar->attributes as $key => $value) {
            $pExemplar->getFilteredAttributes()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($key)
                ->setValue(Converter::convertAnyValue($value));
        }
        $pExemplar->setTimeUnixNano($exemplar->timestamp);
        if ($exemplar->spanContext) {
            $pExemplar->setTraceId(Converter::traceId($exemplar->spanContext, $format));
            $pExemplar->setSpanId(Converter::spanId($exemplar->spanContext, $format));
        }
        if (is_int($exemplar->value)) {
            $pExemplar->setAsInt($exemplar->value);
        }
        if (is_float($exemplar->value)) {
            $pExemplar->setAsDouble($exemplar->value);
        }

        return $pExemplar;
    }
}
