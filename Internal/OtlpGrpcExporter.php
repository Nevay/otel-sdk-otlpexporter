<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpException;
use Amp\Http\Client\Request;
use Amp\Http\Client\Response;
use Amp\Http\Client\SocketException;
use Amp\Http\Http2\Http2ConnectionException;
use Amp\TimeoutCancellation;
use Composer\InstalledVersions;
use Google\Protobuf\Internal\Message;
use InvalidArgumentException;
use JetBrains\PhpStorm\ExpectedValues;
use Nevay\OTelSDK\Common\Internal\Export\Exporter;
use Nevay\OTelSDK\Otlp\ProtobufFormat;
use Nevay\Sync\Internal\LocalSemaphore;
use Nevay\Sync\Internal\Semaphore;
use OpenTelemetry\API\Metrics\CounterInterface;
use OpenTelemetry\API\Metrics\HistogramInterface;
use OpenTelemetry\API\Metrics\UpDownCounterInterface;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use Throwable;
use function Amp\async;
use function Amp\delay;
use function array_key_last;
use function assert;
use function extension_loaded;
use function hrtime;
use function max;
use function mt_rand;
use function pack;
use function sprintf;
use function strlen;
use function strtotime;
use function substr;
use function time;
use function trim;
use function unpack;
use const PHP_INT_MAX;

/**
 * @internal
 *
 * @template T
 * @template P of Message
 * @template R of Message
 * @implements Exporter<T>
 */
abstract class OtlpGrpcExporter implements Exporter {

    private readonly HttpClient $client;
    private readonly UriInterface $endpoint;

    private readonly ?string $compression;
    private readonly array $headers;
    private readonly float $timeout;
    private readonly int $retryDelay;
    private readonly int $maxRetries;
    private readonly int $maxConcurrency;
    private readonly Semaphore $semaphore;
    private readonly LoggerInterface $logger;

    /** @var array<int, Future> */
    private array $pending = [];

    private readonly string $responseClass;

    private readonly UpDownCounterInterface $inflight;
    private readonly CounterInterface $exported;
    private readonly HistogramInterface $duration;
    private readonly array $attributes;

    private DeferredCancellation $shutdown;
    private bool $closed = false;

    /**
     * @param string<R> $responseClass
     */
    public function __construct(
        string $responseClass,
        HttpClient $client,
        UriInterface $endpoint,
        #[ExpectedValues(values: ['gzip', null])]
        ?string $compression,
        array $headers,
        float $timeout,
        int $retryDelay,
        int $maxRetries,
        int $maxConcurrency,
        LoggerInterface $logger,
        UpDownCounterInterface $inflight,
        CounterInterface $exported,
        HistogramInterface $duration,
        string $type,
        string $name,
    ) {
        if ($timeout < 0) {
            throw new InvalidArgumentException(sprintf('Timeout (%s) must be greater than or equal to zero', $timeout));
        }
        if ($retryDelay < 0) {
            throw new InvalidArgumentException(sprintf('Retry delay (%d) must be greater than or equal to zero', $retryDelay));
        }
        if ($maxRetries < 0) {
            throw new InvalidArgumentException(sprintf('Maximum retry count (%d) must be greater than or equal to zero', $maxRetries));
        }

        $this->responseClass = $responseClass;
        $this->client = $client;
        $this->endpoint = $endpoint;
        $this->compression = $compression;
        $this->headers = $headers;
        $this->timeout = $timeout;
        $this->retryDelay = $retryDelay;
        $this->maxRetries = $maxRetries;
        $this->maxConcurrency = $maxConcurrency;
        $this->semaphore = new LocalSemaphore();
        $this->logger = $logger;
        $this->inflight = $inflight;
        $this->exported = $exported;
        $this->duration = $duration;
        $this->shutdown = new DeferredCancellation();

        $this->attributes = [
            'otel.component.name' => $name,
            'otel.component.type' => $type,
            'server.address' => $endpoint->getHost(),
            'server.port' => $endpoint->getPort() ?? match ($endpoint->getScheme()) {
                'https' => 443,
                'http' => 80,
                default => null,
            },
        ];
    }

    /**
     * @param iterable<T> $batch
     * @param ProtobufFormat $format
     * @return RequestPayload<P>
     */
    protected abstract function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload;

    /**
     * @param R $message
     */
    protected abstract function convertResponse(Message $message): ?PartialSuccess;

    /**
     * @param iterable<T> $batch
     * @return Future<bool>
     */
    public function export(iterable $batch, ?Cancellation $cancellation = null): Future {
        if ($this->closed) {
            return Future::complete(false);
        }

        $payload = $this->convertPayload($batch, ProtobufFormat::Protobuf);
        if (!$count = $payload->items) {
            return Future::complete(true);
        }

        unset($batch);
        $request = $this->prepareRequest($payload->message);
        $cancellation = $this->cancellation($cancellation);

        unset($payload);
        assert($this->semaphore->availablePermits($this->maxConcurrency), 'Export() should not be be called concurrently with other Export calls for the same exporter instance.');
        $this->semaphore->acquire(PHP_INT_MAX);

        $future = async(function(Request $request, Cancellation $cancellation) use ($count): bool {
            $this->inflight->add($count, $this->attributes);

            $start = hrtime(true);
            try {
                $response = $this->sendRequest($request, $cancellation);
                unset($request, $cancellation);

                $partialSuccess = $this->mapResponse($response);

                $this->duration->record((hrtime(true) - $start) / 1e9, ['rpc.grpc.status_code' => 0, ...$this->attributes]);
            } catch (Throwable $e) {
                $attributes = $this->attributes;
                $attributes['error.type'] = $e::class;
                if ($e instanceof GrpcException) {
                    $attributes['error.type'] = $e->status->name;
                    $attributes['rpc.grpc.status_code'] = $e->status->value;
                }
                $this->duration->record((hrtime(true) - $start) / 1e9, $attributes);
                $this->exported->add($count, $attributes);
                $this->logger->warning('Export failure: {exception}', ['exception' => $e, ...$attributes]);

                return false;
            } finally {
                $this->inflight->add(-$count, $this->attributes);
            }

            $partialSuccess ??= new PartialSuccess('');

            $this->exported->add($count - $partialSuccess->rejectedItems, $this->attributes);
            $this->exported->add($partialSuccess->rejectedItems, ['error.type' => 'rejected', ...$this->attributes]);

            if ($partialSuccess->rejectedItems || $partialSuccess->errorMessage) {
                $this->logger->warning('Export partial success with warnings/suggestions', ['rejected_items' => $partialSuccess->rejectedItems, 'error_message' => $partialSuccess->errorMessage]);
            }

            return true;
        }, $request, $cancellation);

        $suspension = EventLoop::getSuspension();
        $id = array_key_last($this->pending) + 1;
        $this->pending[$id] = $future->finally(function() use ($id, &$suspension): void {
            unset($this->pending[$id]);
            $this->semaphore->release();

            try {
                $suspension?->resume();
            } catch (Throwable) {}
        });

        unset($request, $cancellation);
        if ($this->semaphore->acquire($this->maxConcurrency)) {
            $this->semaphore->release();
        }
        $suspension = null;

        return $future;
    }

    public function shutdown(?Cancellation $cancellation = null): bool {
        if ($this->closed) {
            return false;
        }

        $this->closed = true;
        $cancellationId = $cancellation?->subscribe($this->shutdown->cancel(...));
        foreach (Future::iterate($this->pending, $cancellation) as $ignored) {}

        $cancellation?->unsubscribe($cancellationId);

        return true;
    }

    public function forceFlush(?Cancellation $cancellation = null): bool {
        if ($this->closed) {
            return false;
        }

        foreach (Future::iterate($this->pending, $cancellation) as $ignored) {}

        return true;
    }

    private function mapResponse(Response $response): ?PartialSuccess {
        $body = $response->getBody()->buffer();
        $prefix = unpack('Ccompressed/Nlength', $body);
        $payload = substr($body, 5, $prefix['length']);
        unset($body);

        if ($prefix['compressed']) {
            /** @noinspection PhpComposerExtensionStubsInspection */
            $payload = match ($response->getHeader('grpc-encoding')) {
                'gzip' => \gzdecode($payload),
            };
        }

        $message = new $this->responseClass;
        Serializer::hydrate($message, $payload, ProtobufFormat::Protobuf);

        return $this->convertResponse($message);
    }

    private function prepareRequest(Message $message): Request {
        $payload = Serializer::serialize($message, ProtobufFormat::Protobuf);
        $request = new Request($this->endpoint, 'POST');
        /** @noinspection PhpParamsInspection */
        $request->setProtocolVersions(['2']);
        $request->setHeader('user-agent', self::userAgent());
        $request->setHeader('content-type', 'application/grpc+proto');
        if ($this->compression === 'gzip' && extension_loaded('zlib')) {
            $payload = \gzencode($payload);
            $request->setHeader('grpc-encoding', 'gzip');
            $request->setHeader('grpc-accept-encoding', 'gzip');
        }
        foreach ($this->headers as $header => $value) {
            $request->addHeader($header, $value);
        }

        $prefix = pack('CN', +$request->hasHeader('grpc-encoding'), strlen($payload));
        $request->setBody($prefix . $payload);

        return $request;
    }

    /**
     * @throws HttpException
     */
    private function sendRequest(Request $request, Cancellation $cancellation): Response {
        $r = $request;
        $c = $cancellation;
        unset($request, $cancellation);

        for ($retries = 0;;) {
            $response = null;
            try {
                $response = $this->client->request(clone $r, new CompositeCancellation($c, new TimeoutCancellation($this->timeout)));
                $trailers = $response->getTrailers()->await($c);

                $status = $trailers->hasHeader('grpc-status')
                    ? GrpcStatus::tryFrom(+$trailers->getHeader('grpc-status')) ?? GrpcStatus::Unknown
                    : match ($response->getStatus()) {
                        // https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md
                        400 => GrpcStatus::Internal,
                        401 => GrpcStatus::Unauthenticated,
                        403 => GrpcStatus::PermissionDenied,
                        404 => GrpcStatus::Unimplemented,
                        429,
                        502,
                        503,
                        504 => GrpcStatus::Unavailable,
                        default => GrpcStatus::Unknown,
                    };

                if ($status === GrpcStatus::Ok) {
                    return $response;
                }
                $retryable = match ($status) {
                    GrpcStatus::Cancelled,
                    GrpcStatus::DeadlineExceeded,
                    GrpcStatus::Aborted,
                    GrpcStatus::OutOfRange,
                    GrpcStatus::Unavailable,
                    GrpcStatus::DataLoss,
                        => true,
                    default => false,
                };

                $e = new GrpcException($status, $trailers->getHeader('grpc-message'));
                if (!$retryable) {
                    throw $e;
                }
                $this->logger->info('Retryable gRPC status during export {exception}', ['exception' => $e, 'status' => $status->name, 'retry' => $retries]);
            } catch (SocketException | Http2ConnectionException | CancelledException $e) {
                $this->logger->info('Retryable exception during export {exception}', ['exception' => $e, 'retry' => $retries]);
            }

            if (++$retries === $this->maxRetries) {
                throw $e;
            }

            $delay = $this->retryDelay << $retries - 1;
            $delay = mt_rand($delay >> 1, $delay) / 1000;
            $delay = max($delay, $this->parseRetryAfter($response));

            unset($response);
            try {
                delay($delay, cancellation: $c);
            } catch (CancelledException) {
                throw $e;
            }

            unset($e);
        }
    }

    private function parseRetryAfter(?Response $response): int {
        if (!$retryAfter = $response?->getHeader('retry-after')) {
            return 0;
        }

        $retryAfter = trim($retryAfter, " \t");
        if ($retryAfter === (string) (int) $retryAfter) {
            return (int) $retryAfter;
        }

        if (($time = strtotime($retryAfter)) !== false) {
            return $time - time();
        }

        return 0;
    }

    private function cancellation(?Cancellation $cancellation): Cancellation {
        return $cancellation
            ? new CompositeCancellation(
                $cancellation,
                $this->shutdown->getCancellation(),
            )
            : $this->shutdown->getCancellation();
    }

    private static function userAgent(): string {
        return 'TBachert-OTLP-Exporter-PHP/' . InstalledVersions::getPrettyVersion('tbachert/otel-sdk-otlpexporter');
    }
}
