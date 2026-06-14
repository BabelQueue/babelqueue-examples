<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use App\RdKafkaConsumerClient;
use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaMessage;

$client = new RdKafkaConsumerClient(getenv('KAFKA_BROKERS') ?: 'localhost:9092', 'orders', 'php-workers');
$consumer = new KafkaConsumer($client);

echo "[php] consuming 'orders' over ext-rdkafka (process-then-commit)...\n";

$seen = 0;
$consumer->consume(
    function (KafkaMessage $m) use (&$seen): void {
        $seen++;
        printf(
            "[php] %-26s data=%s  from lang=%s  trace=%s  attempts=%d\n",
            $m->getUrn(),
            json_encode($m->getData(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
            (string) ($m->getMeta()['lang'] ?? '?'),
            $m->getTraceId(),
            $m->attempts(),
        );
    },
    function () use (&$seen): bool {
        return $seen >= 4;
    },
);

$client->close();
echo "[php] done — consumed {$seen} message(s), offsets committed.\n";
