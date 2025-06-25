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
use OpenTelemetry\API\Metrics\CounterInterface;
use OpenTelemetry\API\Metrics\HistogramInterface;
use OpenTelemetry\API\Metrics\UpDownCounterInterface;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;
use Throwable;
use function Amp\async;
use function Amp\delay;
use function array_key_last;
use function extension_loaded;
use function hrtime;
use function in_array;
use function max;
use function sprintf;
use function strtotime;
use function time;
use function trim;

/**
 * @internal
 *
 * @template T
 * @template P of Message
 * @template R of Message
 * @implements Exporter<T>
 */
abstract class OtlpHttpExporter implements Exporter {

    private readonly HttpClient $client;
    private readonly UriInterface $endpoint;

    private readonly ProtobufFormat $format;
    private readonly ?string $compression;
    private readonly array $headers;
    private readonly float $timeout;
    private readonly int $retryDelay;
    private readonly int $maxRetries;
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
        ProtobufFormat $format,
        #[ExpectedValues(values: ['gzip', null])]
        ?string $compression,
        array $headers,
        float $timeout,
        int $retryDelay,
        int $maxRetries,
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
        $this->format = $format;
        $this->compression = $compression;
        $this->headers = $headers;
        $this->timeout = $timeout;
        $this->retryDelay = $retryDelay;
        $this->maxRetries = $maxRetries;
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

        $payload = $this->convertPayload($batch, $this->format);
        if (!$count = $payload->items) {
            return Future::complete(true);
        }

        unset($batch);
        $request = $this->prepareRequest($payload->message);
        $cancellation = $this->cancellation($cancellation);

        $future = async(function(Request $request, Cancellation $cancellation) use ($count): bool {
            $this->inflight->add($count, $this->attributes);

            $start = hrtime(true);
            try {
                $response = $this->sendRequest($request, $cancellation);
                unset($request, $cancellation);

                $partialSuccess = $this->mapResponse($response);

                $this->duration->record((hrtime(true) - $start) / 1e9, ['http.response.status_code' => $response->getStatus(), ...$this->attributes]);
            } catch (Throwable $e) {
                $attributes = $this->attributes;
                $attributes['error.type'] = $e::class;
                if ($e instanceof HttpException && $e->getCode()) {
                    $attributes['error.type'] = $e->getCode();
                    $attributes['http.response.status_code'] = $e->getCode();
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

        $id = array_key_last($this->pending) + 1;
        $this->pending[$id] = $future->finally(function() use ($id): void {
            unset($this->pending[$id]);
        });

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
        $message = new $this->responseClass;
        Serializer::hydrate($message, $response->getBody()->buffer(), $this->format);

        return $this->convertResponse($message);
    }

    private function prepareRequest(Message $message): Request {
        $payload = Serializer::serialize($message, $this->format);
        $request = new Request($this->endpoint, 'POST');
        $request->setHeader('user-agent', self::userAgent());
        $request->setHeader('content-type', Serializer::contentType($this->format));
        if ($this->compression === 'gzip' && extension_loaded('zlib')) {
            $payload = \gzencode($payload);
            $request->setHeader('content-encoding', 'gzip');
        }
        foreach ($this->headers as $header => $value) {
            $request->addHeader($header, $value);
        }
        $request->setBody($payload);

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

                if ($response->getStatus() >= 200 && $response->getStatus() < 300) {
                    return $response;
                }

                $e = new HttpException($response->getReason(), $response->getStatus());
                if ($response->getStatus() >= 400 && $response->getStatus() < 600 && !in_array($response->getStatus(), [429, 502, 503, 504], true)) {
                    throw $e;
                }
                $this->logger->info('Retryable HTTP status during export {exception}', ['exception' => $e, 'status' => $response->getStatus(), 'retry' => $retries]);
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
