<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

use Nevay\OTelSDK\Common\InstrumentationScope;
use Nevay\OTelSDK\Common\Resource;
use Nevay\OTelSDK\Otlp\ProtobufFormat;
use OpenTelemetry\API\Trace\SpanContextInterface;
use Opentelemetry\Proto;
use WeakMap;
use function array_is_list;
use function extension_loaded;
use function gettype;
use function preg_match;
use function serialize;

/**
 * @internal
 */
final class Converter {

    public static function instrumentationScopeId(InstrumentationScope $instrumentationScope): string {
        static $cache = new WeakMap();
        return $cache[$instrumentationScope] ??= serialize([
            $instrumentationScope->name,
            $instrumentationScope->version,
            $instrumentationScope->schemaUrl,
        ]);
    }

    public static function convertResource(Resource $resource): Proto\Resource\V1\Resource {
        $pResource = new Proto\Resource\V1\Resource();
        foreach ($resource->attributes as $key => $value) {
            $pResource->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($key)
                ->setValue(self::convertAnyValue($value));
        }
        $pResource->setDroppedAttributesCount($resource->attributes->getDroppedAttributesCount());

        return $pResource;
    }

    public static function convertInstrumentationScope(InstrumentationScope $instrumentationScope): Proto\Common\V1\InstrumentationScope {
        $pInstrumentationScope = new Proto\Common\V1\InstrumentationScope();
        $pInstrumentationScope->setName($instrumentationScope->name);
        $pInstrumentationScope->setVersion((string) $instrumentationScope->version);
        foreach ($instrumentationScope->attributes as $key => $value) {
            $pInstrumentationScope->getAttributes()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($key)
                ->setValue(self::convertAnyValue($value));
        }
        $pInstrumentationScope->setDroppedAttributesCount($instrumentationScope->attributes->getDroppedAttributesCount());

        return $pInstrumentationScope;
    }

    public static function convertAnyValue($value): Proto\Common\V1\AnyValue {
        $pAnyValue = new Proto\Common\V1\AnyValue();
        match (gettype($value)) {
            'boolean'  => $pAnyValue->setBoolValue($value),
            'integer'  => $pAnyValue->setIntValue($value),
            'double'   => $pAnyValue->setDoubleValue($value),
            'string'   => self::isUtf8($value)
                ? $pAnyValue->setStringValue($value)
                : $pAnyValue->setBytesValue($value),
            'array'    => array_is_list($value)
                ? $pAnyValue->setArrayValue(self::convertArrayValue($value))
                : $pAnyValue->setKvlistValue(self::convertKeyValueList($value)),
            default    => null,
        };

        return $pAnyValue;
    }

    private static function isUtf8(string $value): bool {
        return extension_loaded('mbstring')
            ? \mb_check_encoding($value, 'UTF-8')
            : (bool) preg_match('//u', $value);
    }

    private static function convertArrayValue(iterable $value): Proto\Common\V1\ArrayValue {
        $pArrayValue = new Proto\Common\V1\ArrayValue();
        foreach ($value as $v) {
            $pArrayValue->getValues()[] = self::convertAnyValue($v);
        }

        return $pArrayValue;
    }

    private static function convertKeyValueList(iterable $value): Proto\Common\V1\KeyValueList {
        $pKeyValueList = new Proto\Common\V1\KeyValueList();
        foreach ($value as $k => $v) {
            $pKeyValueList->getValues()[] = (new Proto\Common\V1\KeyValue())
                ->setKey($k)
                ->setValue(self::convertAnyValue($v));
        }

        return $pKeyValueList;
    }

    public static function traceId(SpanContextInterface $spanContext, ProtobufFormat $format): string {
        return match ($format) {
            ProtobufFormat::Protobuf => $spanContext->getTraceIdBinary(),
            ProtobufFormat::Json => base64_decode($spanContext->getTraceId()),
        };
    }

    public static function spanId(SpanContextInterface $spanContext, ProtobufFormat $format): string {
        return match ($format) {
            ProtobufFormat::Protobuf => $spanContext->getSpanIdBinary(),
            ProtobufFormat::Json => base64_decode($spanContext->getSpanId()),
        };
    }
}
