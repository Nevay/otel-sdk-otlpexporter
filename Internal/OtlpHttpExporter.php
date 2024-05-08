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
use Nevay\OTelSDK\Common\Internal\Export\Exception\TransientExportException;
use Nevay\OTelSDK\Common\Internal\Export\Exporter;
use Nevay\OTelSDK\Otlp\ProtobufFormat;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;
use function Amp\async;
use function Amp\delay;
use function array_key_last;
use function extension_loaded;
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
    private readonly ?LoggerInterface $logger;

    /** @var array<int, Future> */
    private array $pending = [];

    private readonly string $responseClass;

    private DeferredCancellation $shutdown;
    private bool $closed = false;

    /**
     * @param string<R> $responseClass
     */
    public function __construct(
        string $responseClass,
        HttpClient $client,
        UriInterface $endpoint,
        ProtobufFormat $format = ProtobufFormat::Protobuf,
        #[ExpectedValues(values: ['gzip', null])]
        ?string $compression = null,
        array $headers = [],
        float $timeout = 10.,
        int $retryDelay = 5000,
        int $maxRetries = 5,
        ?LoggerInterface $logger = null,
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
        $this->shutdown = new DeferredCancellation();
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
        if (!$payload->items) {
            return Future::complete(true);
        }

        unset($batch);
        $request = $this->prepareRequest($payload->message);
        $cancellation = $this->cancellation($cancellation);

        $future = async($this->sendRequest(...), $request, $cancellation)
            ->map($this->mapResponse(...));

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

    private function mapResponse(Response $response): bool {
        $message = new $this->responseClass;
        Serializer::hydrate($message, $response->getBody()->buffer(), $this->format);

        $partialSuccess = $this->convertResponse($message);
        if ($partialSuccess?->rejectedItems) {
            $this->logger?->error('Export partial success', [
                'rejected_items' => $partialSuccess->rejectedItems,
                'error_message' => $partialSuccess->errorMessage,
            ]);
        }
        if ($partialSuccess?->errorMessage) {
            $this->logger?->warning('Export success with warnings/suggestions', [
                'error_message' => $partialSuccess->errorMessage
            ]);
        }

        return true;
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
            $response = $e = null;
            try {
                $response = $this->client->request(clone $r, new CompositeCancellation($c, new TimeoutCancellation($this->timeout)));

                if ($response->getStatus() >= 200 && $response->getStatus() < 300) {
                    return $response;
                }
                if ($response->getStatus() >= 400 && $response->getStatus() < 600 && !in_array($response->getStatus(), [429, 502, 503, 504], true)) {
                    throw new HttpException($response->getReason(), $response->getStatus());
                }
            } catch (SocketException | Http2ConnectionException | CancelledException $e) {
                $this->logger?->info('Retryable exception during export {exception}', ['exception' => $e, 'retry' => $retries]);
            }

            if (++$retries === $this->maxRetries) {
                throw new TransientExportException('Too many retries', 0, $e);
            }

            $delay = $this->retryDelay << $retries - 1;
            $delay = mt_rand($delay >> 1, $delay) / 1000;
            $delay = max($delay, $this->parseRetryAfter($response));

            unset($response, $e);
            delay($delay, cancellation: $c);
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
