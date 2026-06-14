<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use App\StompPhpClient;
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Transport\StompTransport;
use Stomp\Client;
use Stomp\StatefulStomp;

$url = getenv('ARTEMIS_STOMP') ?: 'tcp://localhost:61613';
$client = new Client($url);
$client->setLogin('artemis', 'artemis');
$transport = new StompTransport(new StompPhpClient(new StatefulStomp($client)), 'orders');

$messages = [
    ['urn:babel:orders:created', ['order_id' => 2001, 'amount' => 19.99]],
    ['urn:babel:orders:created', ['order_id' => 2002, 'amount' => 39.98]],
    ['urn:babel:orders:created', ['order_id' => 2003, 'amount' => 59.97]],
    ['urn:babel:orders:shipped', ['order_id' => 2002, 'carrier' => 'DHL Express ✈']],
];

foreach ($messages as [$urn, $data]) {
    $id = $transport->publish(EnvelopeCodec::encode(EnvelopeCodec::make($urn, $data, 'orders')));
    printf("[php] published %s  id=%s  lang=php\n", $urn, $id);
}

$client->disconnect();
echo "[php] 4 message(s) on 'orders' over STOMP.\n";
