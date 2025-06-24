<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

/**
 * @internal
 */
enum GrpcStatus: int {

    case Ok = 0;
    case Cancelled = 1;
    case Unknown = 2;
    case InvalidArgument = 3;
    case DeadlineExceeded = 4;
    case NotFound = 5;
    case AlreadyExists = 6;
    case PermissionDenied = 7;
    case ResourceExhausted = 8;
    case FailedPrecondition = 9;
    case Aborted = 10;
    case OutOfRange = 11;
    case Unimplemented = 12;
    case Internal = 13;
    case Unavailable = 14;
    case DataLoss = 15;
    case Unauthenticated = 16;
}
