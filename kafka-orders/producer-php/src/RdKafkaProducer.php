<?php

declare(strict_types=1);

namespace App;

use BabelQueue\Transport\KafkaProducer;
use RdKafka\Producer;

/** One-line adapter binding BabelQueue's KafkaProducer seam to ext-rdkafka's producev(). */
final class RdKafkaProducer implements KafkaProducer
{
    public function __construct(private readonly Producer $producer)
    {
    }

    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
    {
        $this->producer->newTopic($topic)->producev(
            RD_KAFKA_PARTITION_UA,
            0,
            $payload,
            null,            // key
            $headers,
            $timestampMs,
        );
        $this->producer->poll(0);
    }
}
