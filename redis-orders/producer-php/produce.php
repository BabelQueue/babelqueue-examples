<?php

/**
 * Producer (PHP) — publishes canonical BabelQueue envelopes to a shared Redis
 * queue that a consumer in *any other language* can read.
 *
 *   composer install
 *   php produce.php
 *
 * The framework-less core builds the envelope (EnvelopeCodec) and the bundled
 * RedisTransport does a plain RPUSH onto the "orders" list — the exact same
 * reliable-queue convention the Go/Python SDKs use, so they interoperate.
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\PolyglotJob;
use BabelQueue\Transport\RedisTransport;
use Predis\Client;

/** A generic producible message: a stable URN + a pure-JSON payload. */
final class OrderMessage implements PolyglotJob
{
    /**
     * @param  array<string, mixed>  $data
     */
    public function __construct(
        private readonly string $urn,
        private readonly array $data,
    ) {
    }

    public function getBabelUrn(): string
    {
        return $this->urn;
    }

    /**
     * @return array<string, mixed>
     */
    public function toPayload(): array
    {
        return $this->data;
    }
}

$brokerUrl = getenv('BROKER_URL') ?: 'redis://localhost:6379/0';
$queue = 'orders';

$transport = new RedisTransport(new Client($brokerUrl), $queue);

$messages = [
    new OrderMessage('urn:babel:orders:created', ['order_id' => 1042, 'amount' => 99.90, 'currency' => 'USD']),
    new OrderMessage('urn:babel:orders:created', ['order_id' => 1043, 'amount' => 12.50, 'currency' => 'EUR']),
    new OrderMessage('urn:babel:catalog:item.indexed', ['sku' => 'WIDGET-1', 'title' => 'Café Widget ☕']),
];

foreach ($messages as $message) {
    $envelope = EnvelopeCodec::fromJob($message, $queue);
    $transport->publish(EnvelopeCodec::encode($envelope), $queue);

    printf(
        "[php] published %s  id=%s  data=%s\n",
        $envelope['job'],
        $envelope['meta']['id'],
        json_encode($envelope['data'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
    );
}

printf(
    "[php] %d message(s) on the '%s' Redis list — now run the Go consumer.\n",
    count($messages),
    $queue,
);
