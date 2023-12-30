<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp\Internal;

use Google\Protobuf\Internal\Message;

/**
 * @internal
 *
 * @template P of Message
 */
final class RequestPayload {

    /**
     * @param P $message
     */
    public function __construct(
        public readonly Message $message,
        public readonly int $items,
    ) {}
}
