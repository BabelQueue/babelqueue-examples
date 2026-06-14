<?php

declare(strict_types=1);

namespace App;

use BabelQueue\Transport\KafkaConsumerClient;
use RdKafka\Conf;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message;

/**
 * Adapter binding BabelQueue's KafkaConsumerClient seam to ext-rdkafka's high-level KafkaConsumer.
 * Manual offset commit (enable.auto.commit=false) gives §6 process-then-commit: receive() polls a
 * record and retains it; commit() commits its offset only after the handler succeeds.
 */
final class RdKafkaConsumerClient implements KafkaConsumerClient
{
    private RdKafkaConsumer $consumer;

    private ?Message $last = null;

    public function __construct(string $brokers, string $topic, string $group)
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $brokers);
        $conf->set('group.id', $group);
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('enable.auto.commit', 'false');

        $this->consumer = new RdKafkaConsumer($conf);
        $this->consumer->subscribe([$topic]);
    }

    public function receive(): ?array
    {
        $message = $this->consumer->consume(1000);

        if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            return null; // PARTITION_EOF / TIMED_OUT → nothing available right now
        }

        $this->last = $message;

        $headers = [];
        foreach (($message->headers ?? []) as $key => $value) {
            $headers[(string) $key] = (string) $value;
        }

        return ['payload' => (string) $message->payload, 'headers' => $headers];
    }

    public function commit(): void
    {
        if ($this->last !== null) {
            $this->consumer->commit($this->last); // synchronous offset commit
        }
    }

    public function close(): void
    {
        $this->consumer->close();
    }
}
