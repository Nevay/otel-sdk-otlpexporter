<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

/**
 * @internal
 */
final class PartialSuccess {

    public function __construct(
        public readonly string $errorMessage,
        public readonly int $rejectedItems = 0,
    ) {}
}
