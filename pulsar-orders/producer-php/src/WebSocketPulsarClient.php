<?php

declare(strict_types=1);

namespace App;

use BabelQueue\Transport\PulsarWebSocketClient;
use RuntimeException;
use WebSocket\Client;

/**
 * Adapter binding BabelQueue's PulsarWebSocketClient seam to the pure-PHP `textalk/websocket`
 * client. It derives the producer endpoint from the topic, base64-encodes the envelope into the
 * frame's `payload`, attaches the §5 `bq-` properties, and checks the producer ack — throwing on a
 * non-`ok` result. It never sets `eventTime` (publishTime is broker-set; the body is authoritative).
 */
final class WebSocketPulsarClient implements PulsarWebSocketClient
{
    /** @var array<string, Client> */
    private array $producers = [];

    private int $context = 0;

    public function __construct(private readonly string $baseUrl = 'ws://localhost:8080/ws/v2')
    {
    }

    public function publish(string $topic, string $payload, array $properties): void
    {
        $client = $this->producerFor($topic);
        $client->send((string) json_encode([
            'payload'    => base64_encode($payload),
            'properties' => $properties,
            'context'    => (string) ++$this->context,
        ]));

        /** @var array<string, mixed> $ack */
        $ack = json_decode((string) $client->receive(), true);
        if (($ack['result'] ?? null) !== 'ok') {
            throw new RuntimeException('Pulsar WebSocket publish failed: ' . (string) ($ack['result'] ?? 'no ack'));
        }
    }

    private function producerFor(string $topic): Client
    {
        if (!isset($this->producers[$topic])) {
            $path = str_replace('persistent://', 'persistent/', $topic);
            $this->producers[$topic] = new Client(sprintf('%s/producer/%s', $this->baseUrl, $path), ['timeout' => 20]);
        }

        return $this->producers[$topic];
    }

    public function close(): void
    {
        foreach ($this->producers as $client) {
            $client->close();
        }
        $this->producers = [];
    }
}
