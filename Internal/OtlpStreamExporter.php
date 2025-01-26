<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

use Amp\ByteStream\WritableStream;
use Amp\Cancellation;
use Amp\Future;
use Google\Protobuf\Internal\Message;
use Nevay\OTelSDK\Common\Internal\Export\Exporter;
use Nevay\OTelSDK\Otlp\ProtobufFormat;
use OpenTelemetry\API\Metrics\CounterInterface;
use OpenTelemetry\API\Metrics\UpDownCounterInterface;
use Psr\Log\LoggerInterface;
use Throwable;
use function Amp\async;

/**
 * @internal
 *
 * @template T
 * @template P of Message
 * @implements Exporter<T>
 */
abstract class OtlpStreamExporter implements Exporter {

    private readonly ProtobufFormat $format;
    private ?WritableStream $stream;
    private ?Future $write = null;
    private readonly LoggerInterface $logger;

    private readonly UpDownCounterInterface $inflight;
    private readonly CounterInterface $exported;
    private readonly string $type;
    private readonly string $name;

    public function __construct(
        WritableStream $stream,
        LoggerInterface $logger,
        UpDownCounterInterface $inflight,
        CounterInterface $exported,
        string $type,
        string $name,
    ) {
        $this->format = ProtobufFormat::Json;
        $this->stream = $stream;
        $this->logger = $logger;
        $this->inflight = $inflight;
        $this->exported = $exported;
        $this->type = $type;
        $this->name = $name;
    }

    /**
     * @param iterable<T> $batch
     * @param ProtobufFormat $format
     * @return RequestPayload<P>
     */
    protected abstract function convertPayload(iterable $batch, ProtobufFormat $format): RequestPayload;

    /**
     * @param iterable<T> $batch
     * @return Future<bool>
     */
    public function export(iterable $batch, ?Cancellation $cancellation = null): Future {
        if (!$stream = $this->stream) {
            return Future::complete(false);
        }

        $payload = $this->convertPayload($batch, $this->format);
        if (!$count = $payload->items) {
            return Future::complete(true);
        }

        unset($batch);
        $payload = Serializer::serialize($payload->message, $this->format) . "\n";

        $future = async(function(WritableStream $stream, string $payload) use ($count): bool {
            $this->inflight->add($count, ['otel.sdk.component.name' => $this->name, 'otel.sdk.component.type' => $this->type]);

            try {
                $stream->write($payload);
            } catch (Throwable $e) {
                $this->exported->add($count, ['error.type' => $e::class, 'otel.sdk.component.name' => $this->name, 'otel.sdk.component.type' => $this->type]);
                $this->logger->warning('Export failure: {exception}', ['exception' => $e, 'otel.sdk.component.name' => $this->name, 'otel.sdk.component.type' => $this->type]);

                return false;
            } finally {
                $this->inflight->add(-$count, ['otel.sdk.component.name' => $this->name, 'otel.sdk.component.type' => $this->type]);
            }

            $this->exported->add($count, ['otel.sdk.component.name' => $this->name, 'otel.sdk.component.type' => $this->type]);

            return true;
        }, $stream, $payload);

        return $this->write = $future;
    }

    public function shutdown(?Cancellation $cancellation = null): bool {
        if (!$this->stream) {
            return false;
        }

        $this->stream = null;
        $this->write?->catch(static fn() => null)->await($cancellation);

        return true;
    }

    public function forceFlush(?Cancellation $cancellation = null): bool {
        if (!$this->stream) {
            return false;
        }

        $this->write?->catch(static fn() => null)->await($cancellation);

        return true;
    }
}
