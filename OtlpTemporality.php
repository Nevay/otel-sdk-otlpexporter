<?php declare(strict_types=1);
namespace Nevay\OTelSDK\Otlp;

use Nevay\OTelSDK\Metrics\Data\Temporality;
use Nevay\OTelSDK\Metrics\InstrumentType;
use Nevay\OTelSDK\Metrics\TemporalityResolver;

enum OtlpTemporality implements TemporalityResolver {

    /**
     * Choose cumulative aggregation temporality for all instrument kinds.
     */
    case Delta;
    /**
     * Choose Delta aggregation temporality for Counter, Asynchronous Counter
     * and Histogram instrument kinds, choose Cumulative aggregation for
     * UpDownCounter and Asynchronous UpDownCounter instrument kinds.
     */
    case Cumulative;
    /**
     * This configuration uses Delta aggregation temporality for Synchronous
     * Counter and Histogram and uses Cumulative aggregation temporality for
     * Synchronous UpDownCounter, Asynchronous Counter, and Asynchronous
     * UpDownCounter instrument kinds.
     */
    case LowMemory;

    public function resolveTemporality(InstrumentType $instrumentType): Temporality {
        return match ($this) {
            self::Cumulative => Temporality::Cumulative,

            self::Delta => match ($instrumentType) {
                InstrumentType::Counter,
                InstrumentType::Histogram,
                InstrumentType::Gauge,
                InstrumentType::AsynchronousCounter,
                InstrumentType::AsynchronousGauge,
                    => Temporality::Delta,
                InstrumentType::UpDownCounter,
                InstrumentType::AsynchronousUpDownCounter,
                    => Temporality::Cumulative,
            },

            self::LowMemory => match ($instrumentType) {
                InstrumentType::Counter,
                InstrumentType::Histogram,
                InstrumentType::Gauge,
                    => Temporality::Delta,
                InstrumentType::UpDownCounter,
                InstrumentType::AsynchronousCounter,
                InstrumentType::AsynchronousUpDownCounter,
                InstrumentType::AsynchronousGauge,
                    => Temporality::Cumulative,
            },
        };
    }
}
