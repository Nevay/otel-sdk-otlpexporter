<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp\Internal;

use Google\Protobuf\Descriptor;
use Google\Protobuf\DescriptorPool;
use Google\Protobuf\FieldDescriptor;
use Google\Protobuf\Internal\GPBLabel;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Nevay\OtelSDK\Otlp\ProtobufFormat;
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
            ProtobufFormat::PROTOBUF => 'application/x-protobuf',
            ProtobufFormat::JSON => 'application/json',
        };
    }

    public static function serialize(Message $message, ProtobufFormat $format): string {
        return match ($format) {
            ProtobufFormat::PROTOBUF => $message->serializeToString(),
            ProtobufFormat::JSON => self::postProcessJsonEnumValues($message, $message->serializeToJsonString()),
        };
    }

    public static function hydrate(Message $message, string $payload, ProtobufFormat $format): void {
        match ($format) {
            ProtobufFormat::PROTOBUF => $message->mergeFromString($payload),
            ProtobufFormat::JSON => $message->mergeFromJsonString($payload, ignore_unknown: true),
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
        $pool = DescriptorPool::getGeneratedPool();
        $desc = $pool->getDescriptorByClassName($message::class);
        if (!$desc instanceof Descriptor) {
            return $payload;
        }

        $data = json_decode($payload);
        unset($payload);
        self::traverseDescriptor($data, $desc);

        return json_encode($data, flags: JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    private static function traverseDescriptor(object $data, Descriptor $desc): void {
        for ($i = 0, $n = $desc->getFieldCount(); $i < $n; $i++) {
            $field = $desc->getField($i);
            $name = lcfirst(strtr(ucwords($field->getName(), '_'), ['_' => '']));
            if (!property_exists($data, $name)) {
                continue;
            }

            if ($field->getLabel() === GPBLabel::REPEATED) {
                foreach ($data->$name as $key => $value) {
                    $data->$name[$key] = self::traverseFieldDescriptor($value, $field);
                }
            } else {
                $data->$name = self::traverseFieldDescriptor($data->$name, $field);
            }
        }
    }

    private static function traverseFieldDescriptor(mixed $data, FieldDescriptor $field): mixed {
        switch ($field->getType()) {
            case GPBType::MESSAGE:
                self::traverseDescriptor($data, $field->getMessageType());
                break;
            case GPBType::ENUM:
                $enum = $field->getEnumType();
                for ($i = 0, $n = $enum->getValueCount(); $i < $n; $i++) {
                    if ($data === $enum->getValue($i)->getName()) {
                        return $enum->getValue($i)->getNumber();
                    }
                }
                break;
        }

        return $data;
    }
}
