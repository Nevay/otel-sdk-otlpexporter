<?php declare(strict_types=1);
namespace Nevay\OtelSDK\Otlp;

use Amp\ByteStream\WritableStream;
use Amp\Cancellation;
use Amp\Future;
use Nevay\OtelSDK\Metrics\MetricExporter;
use Nevay\OtelSDK\Otlp\Internal\MetricConverter;
use Nevay\OtelSDK\Otlp\Internal\OtlpStreamExporter;
use Psr\Log\LoggerInterface;

final class OtlpStreamMetricExporter implements MetricExporter {

    private readonly OtlpStreamExporter $exporter;

    public function __construct(WritableStream $stream, ?LoggerInterface $logger = null) {
        $this->exporter = new OtlpStreamExporter($stream, $logger);
    }

    public function export(iterable $batch, ?Cancellation $cancellation = null): Future {
        return $this->exporter->export($batch, MetricConverter::convert(...));
    }

    public function shutdown(?Cancellation $cancellation = null): bool {
        return $this->exporter->shutdown($cancellation);
    }

    public function forceFlush(?Cancellation $cancellation = null): bool {
        return $this->exporter->forceFlush($cancellation);
    }
}
