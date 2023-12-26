<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp\Internal;

use Amp\ByteStream\WritableStream;
use Amp\Cancellation;
use Amp\Future;
use Closure;
use Nevay\OtelSDK\Otlp\ProtobufFormat;
use Psr\Log\LoggerInterface;
use Throwable;
use function Amp\async;

final class OtlpStreamExporter {

    private ?WritableStream $stream;
    private ?LoggerInterface $logger;
    private ?Future $write = null;

    public function __construct(WritableStream $stream, ?LoggerInterface $logger = null) {
        $this->stream = $stream;
        $this->logger = $logger;
    }

    /**
     * @template T
     * @param iterable<T> $batch
     * @param Closure(iterable<T>,ProtobufFormat,?LoggerInterface):void $convert
     * @return Future<bool>
     */
    public function export(iterable $batch, Closure $convert): Future {
        if (!$stream = $this->stream) {
            return Future::complete(false);
        }

        $format = ProtobufFormat::JSON;
        $payload = Serializer::serialize($convert($batch, $format, $this->logger), $format) . "\n";

        $future = async($stream->write(...), $payload)
            ->map(static fn() => true)
            ->catch($this->logException(...));

        return $this->write = $future;
    }

    public function shutdown(?Cancellation $cancellation = null): bool {
        if (!$this->stream) {
            return false;
        }

        $this->stream = null;
        $this->write?->await($cancellation);

        return true;
    }

    public function forceFlush(?Cancellation $cancellation = null): bool {
        if (!$this->stream) {
            return false;
        }

        $this->write?->await($cancellation);

        return true;
    }

    private function logException(Throwable $e): bool {
        $this->logger?->error('Export failure {exception}', ['exception' => $e]);

        return false;
    }
}
