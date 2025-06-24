<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

use Exception;
use function sprintf;

/**
 * @internal
 */
final class GrpcException extends Exception {

    public function __construct(public readonly GrpcStatus $status, ?string $message = null) {
        parent::__construct(sprintf('%s: %s', $status->name, $message), $status->value);
    }
}
