<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp\Internal;

use Nevay\OtelSDK\Common\InstrumentationScope;
use Nevay\OtelSDK\Common\Resource;
use Nevay\OtelSDK\Otlp\ProtobufFormat;
use Nevay\OtelSDK\Trace\ReadableSpan;
use Nevay\OtelSDK\Trace\Span\Kind;
use Nevay\OtelSDK\Trace\Span\Status;
use Opentelemetry\Proto;
use Opentelemetry\Proto\Collector\Trace\V1\ExportTraceServiceRequest;
use function spl_object_id;

/**
 * @internal
 */
final class SpanConverter {

    /**
     * @param iterable<ReadableSpan> $spans
     */
    public static function convert(iterable $spans, ProtobufFormat $format): ExportTraceServiceRequest {
        $pExportTraceServiceRequest = new ExportTraceServiceRequest();

        $resourceSpans = [];
        $scopeSpans = [];
        foreach ($spans as $span) {
            $resource = $span->getResource();
            $instrumentationScope = $span->getInstrumentationScope();

            $resourceId = spl_object_id($resource);
            $instrumentationScopeId = Converter::instrumentationScopeId($instrumentationScope);

            $pResourceSpans = $resourceSpans[$resourceId]
                ??= $pExportTraceServiceRequest->getResourceSpans()[]
                = self::convertResourceSpans($resource);
            $pScopeSpans = $scopeSpans[$resourceId][$instrumentationScopeId]
                ??= $pResourceSpans->getScopeSpans()[]
                = self::convertScopeSpans($instrumentationScope);

            $pScopeSpans->getSpans()[] = self::convertSpan($span, $format);
        }

        return $pExportTraceServiceRequest;
    }

    private static function convertResourceSpans(Resource $resource): Proto\Trace\V1\ResourceSpans {
        $pResourceSpans = new Proto\Trace\V1\ResourceSpans();
        $pResourceSpans->setResource(Converter::convertResource($resource));
        $pResourceSpans->setSchemaUrl((string) $resource->schemaUrl);

        return $pResourceSpans;
    }

    private static function convertScopeSpans(InstrumentationScope $instrumentationScope): Proto\Trace\V1\ScopeSpans {
        $pScopeSpans = new Proto\Trace\V1\ScopeSpans();
        $pScopeSpans->setScope(Converter::convertInstrumentationScope($instrumentationScope));
        $pScopeSpans->setSchemaUrl((string) $instrumentationScope->schemaUrl);

        return $pScopeSpans;
    }

    private static function convertSpan(ReadableSpan $span, ProtobufFormat $format): Proto\Trace\V1\Span {
        $pSpan = new Proto\Trace\V1\Span();
        $pSpan->setTraceId(Converter::traceId($span->getContext(), $format));
        $pSpan->setSpanId(Converter::spanId($span->getContext(), $format));
        $pSpan->setTraceState((string) $span->getContext()->getTraceState());
        if ($span->getParentContext()) {
            $pSpan->setParentSpanId(Converter::spanId($span->getParentContext(), $format));
        }
        $pSpan->setName($span->getName());
        $pSpan->setKind(match($span->getSpanKind()) {
            Kind::Internal => Proto\Trace\V1\Span\SpanKind::SPAN_KIND_INTERNAL,
            Kind::Client   => Proto\Trace\V1\Span\SpanKind::SPAN_KIND_CLIENT,
            Kind::Server   => Proto\Trace\V1\Span\SpanKind::SPAN_KIND_SERVER,
            Kind::Producer => Proto\Trace\V1\Span\SpanKind::SPAN_KIND_PRODUCER,
            Kind::Consumer => Proto\Trace\V1\Span\SpanKind::SPAN_KIND_CONSUMER,
        });
        $pSpan->setStartTimeUnixNano($span->getStartTimestamp());
        $pSpan->setEndTimeUnixNano($span->getEndTimestamp());
        foreach ($span->getAttributes() as $key => $value) {
            $pSpan->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($key)
                ->setValue(Converter::convertAnyValue($value));
        }
        $pSpan->setDroppedAttributesCount($span->getAttributes()->getDroppedAttributesCount());

        foreach ($span->getEvents() as $event) {
            $pSpan->getEvents()[] = $pEvent = new Proto\Trace\V1\Span\Event();
            $pEvent->setTimeUnixNano($event->timestamp);
            $pEvent->setName($event->name);
            foreach ($event->attributes as $key => $value) {
                $pEvent->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                    ->setKey($key)
                    ->setValue(Converter::convertAnyValue($value));
            }
            $pEvent->setDroppedAttributesCount($event->attributes->getDroppedAttributesCount());
        }
        $pSpan->setDroppedEventsCount($span->getDroppedEventsCount());

        foreach ($span->getLinks() as $link) {
            $pSpan->getLinks()[] = $pLink = new Proto\Trace\V1\Span\Link();
            $pLink->setTraceId(Converter::traceId($link->spanContext, $format));
            $pLink->setSpanId(Converter::spanId($link->spanContext, $format));
            $pLink->setTraceState((string) $link->spanContext->getTraceState());
            foreach ($link->attributes as $key => $value) {
                $pLink->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                    ->setKey($key)
                    ->setValue(Converter::convertAnyValue($value));
            }
            $pLink->setDroppedAttributesCount($link->attributes->getDroppedAttributesCount());
        }
        $pSpan->setDroppedLinksCount($span->getDroppedLinksCount());

        $pStatus = new Proto\Trace\V1\Status();
        if ($span->getStatusDescription() !== null) {
            $pStatus->setMessage($span->getStatusDescription());
        }
        $pStatus->setCode(match($span->getStatus()) {
            Status::Unset => Proto\Trace\V1\Status\StatusCode::STATUS_CODE_UNSET,
            Status::Ok    => Proto\Trace\V1\Status\StatusCode::STATUS_CODE_OK,
            Status::Error => Proto\Trace\V1\Status\StatusCode::STATUS_CODE_ERROR,
        });
        $pSpan->setStatus($pStatus);

        return $pSpan;
    }
}
