<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use App\RdKafkaProducer;
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Transport\KafkaTransport;
use RdKafka\Conf;
use RdKafka\Producer;

$conf = new Conf();
$conf->set('metadata.broker.list', getenv('KAFKA_BROKERS') ?: 'localhost:9092');
$producer = new Producer($conf);
$transport = new KafkaTransport(new RdKafkaProducer($producer), 'orders');

$messages = [
    ['urn:babel:orders:created', ['order_id' => 3001, 'amount' => 19.99]],
    ['urn:babel:orders:created', ['order_id' => 3002, 'amount' => 39.98]],
    ['urn:babel:orders:created', ['order_id' => 3003, 'amount' => 59.97]],
    ['urn:babel:orders:shipped', ['order_id' => 3002, 'carrier' => 'DHL Express ✈']],
];

foreach ($messages as [$urn, $data]) {
    $id = $transport->publish(EnvelopeCodec::encode(EnvelopeCodec::make($urn, $data, 'orders')));
    printf("[php] published %s  id=%s  lang=php\n", $urn, $id);
}

$producer->flush(10000);
echo "[php] 4 record(s) on topic 'orders' over ext-rdkafka.\n";
