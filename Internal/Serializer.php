<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

use Google\Protobuf\DescriptorPool;
use Google\Protobuf\Internal\GPBLabel;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Nevay\OTelSDK\Otlp\ProtobufFormat;
use function class_exists;
use function json_decode;
use function json_encode;
use function lcfirst;
use function property_exists;
use function strtr;
use function ucwords;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

/**
 * @internal
 */
final class Serializer {

    public static function contentType(ProtobufFormat $format): string {
        return match ($format) {
            ProtobufFormat::Protobuf => 'application/x-protobuf',
            ProtobufFormat::Json => 'application/json',
        };
    }

    public static function serialize(Message $message, ProtobufFormat $format): string {
        return match ($format) {
            ProtobufFormat::Protobuf => $message->serializeToString(),
            # https://github.com/protocolbuffers/protobuf/pull/12707
            ProtobufFormat::Json => class_exists(\Google\Protobuf\PrintOptions::class)
                ? $message->serializeToJsonString(\Google\Protobuf\PrintOptions::ALWAYS_PRINT_ENUMS_AS_INTS)
                : self::postProcessJsonEnumValues($message, $message->serializeToJsonString()),
        };
    }

    public static function hydrate(Message $message, string $payload, ProtobufFormat $format): void {
        match ($format) {
            ProtobufFormat::Protobuf => $message->mergeFromString($payload),
            ProtobufFormat::Json => $message->mergeFromJsonString($payload, ignore_unknown: true),
        };
    }

    /**
     * Workaround until protobuf exposes `FormatEnumsAsIntegers` option.
     *
     * [JSON Protobuf Encoding](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding):
     * > Values of enum fields MUST be encoded as integer values.
     *
     * @see https://github.com/open-telemetry/opentelemetry-php/issues/978
     * @see https://github.com/protocolbuffers/protobuf/pull/12707
     */
    private static function postProcessJsonEnumValues(Message $message, string $payload): string {
        $data = json_decode($payload);
        unset($payload);
        self::traverseDescriptor($data, $message::class);

        return json_encode($data, flags: JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    /**
     * @param class-string $class
     */
    private static function traverseDescriptor(object $data, string $class): void {
        foreach (self::fields($class) as $name => $field) {
            if (!property_exists($data, $name)) {
                continue;
            }

            if ($field->repeated) {
                foreach ($data->$name as $key => $value) {
                    if ($field->message) {
                        self::traverseDescriptor($value, $field->message);
                    }
                    if ($field->enums) {
                        $data->$name[$key] = $field->enums[$value] ?? $value;
                    }
                }
            } else {
                if ($field->message) {
                    self::traverseDescriptor($data->$name, $field->message);
                }
                if ($field->enums) {
                    $data->$name = $field->enums[$data->$name] ?? $data->$name;
                }
            }
        }
    }

    /**
     * @param class-string $class
     * @return array<string, object{
     *     message: ?class-string,
     *     enums: ?array,
     *     repeated: bool,
     * }>
     */
    private static function fields(string $class): array {
        static $cache = [];
        if ($fields = $cache[$class] ?? null) {
            return $fields;
        }
        if (!$desc = DescriptorPool::getGeneratedPool()->getDescriptorByClassName($class)) {
            return [];
        }

        $fields = [];
        for ($i = 0, $n = $desc->getFieldCount(); $i < $n; $i++) {
            $field = $desc->getField($i);
            $type = $field->getType();
            if ($type !== GPBType::MESSAGE && $type !== GPBType::ENUM) {
                continue;
            }

            $fieldDescriptor = new class {
                public ?string $message = null;
                public ?array $enums = null;
                public bool $repeated;
            };

            if ($type === GPBType::MESSAGE) {
                $fieldDescriptor->message = $field->getMessageType()->getClass();
            }
            if ($type === GPBType::ENUM) {
                $enum = $field->getEnumType();
                $fieldDescriptor->enums = [];
                for ($e = 0, $m = $enum->getValueCount(); $e < $m; $e++) {
                    $value = $enum->getValue($e);
                    $fieldDescriptor->enums[$value->getName()] = $value->getNumber();
                }
            }
            $fieldDescriptor->repeated = $field->getLabel() === GPBLabel::REPEATED;

            $name = lcfirst(strtr(ucwords($field->getName(), '_'), ['_' => '']));
            $fields[$name] = $fieldDescriptor;
        }

        return $cache[$class] = $fields;
    }
}
