<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use App\WebSocketPulsarClient;
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Transport\PulsarTransport;

$client = new WebSocketPulsarClient(getenv('PULSAR_WS') ?: 'ws://localhost:8080/ws/v2');
$transport = new PulsarTransport($client, 'orders');   // topic persistent://public/default/orders

$messages = [
    ['urn:babel:orders:created', ['order_id' => 5001, 'amount' => 19.99]],
    ['urn:babel:orders:created', ['order_id' => 5002, 'amount' => 39.98]],
    ['urn:babel:orders:created', ['order_id' => 5003, 'amount' => 59.97]],
    ['urn:babel:orders:shipped', ['order_id' => 5002, 'carrier' => 'DHL Express ✈']],
];

foreach ($messages as [$urn, $data]) {
    $id = $transport->publish(EnvelopeCodec::encode(EnvelopeCodec::make($urn, $data, 'orders')));
    printf("[php] published %s  id=%s  lang=php\n", $urn, $id);
}

$client->close();
echo "[php] 4 message(s) on topic 'orders' over the Pulsar WebSocket API (pure-PHP textalk/websocket).\n";
