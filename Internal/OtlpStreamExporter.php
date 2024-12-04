<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp\Internal;

use Amp\ByteStream\WritableStream;
use Amp\Cancellation;
use Amp\Future;
use Google\Protobuf\Internal\Message;
use Nevay\OTelSDK\Common\Internal\Export\Exporter;
use Nevay\OTelSDK\Otlp\ProtobufFormat;
use Psr\Log\LoggerInterface;
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

    public function __construct(WritableStream $stream, ?LoggerInterface $logger = null) {
        $this->format = ProtobufFormat::Json;
        $this->stream = $stream;
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
        if (!$payload->items) {
            return Future::complete(true);
        }

        unset($batch);
        $payload = Serializer::serialize($payload->message, $this->format) . "\n";
        $future = async($stream->write(...), $payload)
            ->map(static fn(): bool => true);

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
