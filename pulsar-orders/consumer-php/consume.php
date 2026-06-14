<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use App\WebSocketPulsarConsumerClient;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarMessage;

$client = new WebSocketPulsarConsumerClient(
    getenv('PULSAR_TOPIC') ?: 'persistent://public/default/orders',
    'babelqueue',
    getenv('PULSAR_WS') ?: 'ws://localhost:8080/ws/v2',
);
$consumer = new PulsarConsumer($client);

echo "[php] consuming 'orders' over the Pulsar WebSocket consumer API...\n";

$seen = 0;
$consumer->consume(
    function (PulsarMessage $m) use (&$seen): void {
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
        return $seen >= 4; // stop after the 4 demo messages
    },
);

$client->close();
echo "[php] done — consumed {$seen} message(s), all ACKed.\n";
