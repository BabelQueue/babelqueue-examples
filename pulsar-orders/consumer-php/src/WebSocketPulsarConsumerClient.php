<?php

declare(strict_types=1);

namespace App;

use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use WebSocket\Client;

/**
 * Adapter binding BabelQueue's PulsarWebSocketConsumerClient seam to the pure-PHP `textalk/websocket`
 * client over Pulsar's WebSocket consumer endpoint. It reads consumer frames (base64 payload +
 * properties + redeliveryCount) and sends ack / negative-ack frames back on the same socket.
 */
final class WebSocketPulsarConsumerClient implements PulsarWebSocketConsumerClient
{
    private Client $client;

    public function __construct(
        string $topic = 'persistent://public/default/orders',
        string $subscription = 'babelqueue',
        string $baseUrl = 'ws://localhost:8080/ws/v2',
    ) {
        $path = str_replace('persistent://', 'persistent/', $topic);
        $url = sprintf('%s/consumer/%s/%s?subscriptionType=Shared', $baseUrl, $path, $subscription);
        $this->client = new Client($url, ['timeout' => 5]);
    }

    public function receive(): ?array
    {
        try {
            $raw = $this->client->receive();
        } catch (\Throwable) {
            return null; // read timeout → no message available right now
        }

        /** @var array<string, mixed> $frame */
        $frame = json_decode((string) $raw, true);
        if (! is_array($frame) || ! isset($frame['messageId'], $frame['payload'])) {
            return null;
        }

        return [
            'messageId' => (string) $frame['messageId'],
            'payload' => (string) base64_decode((string) $frame['payload'], true),
            'properties' => (array) ($frame['properties'] ?? []),
            'redeliveryCount' => (int) ($frame['redeliveryCount'] ?? 0),
        ];
    }

    public function acknowledge(string $messageId): void
    {
        $this->client->send((string) json_encode(['messageId' => $messageId]));
    }

    public function negativeAcknowledge(string $messageId): void
    {
        $this->client->send((string) json_encode(['messageId' => $messageId, 'type' => 'negativeAcknowledge']));
    }

    public function close(): void
    {
        $this->client->close();
    }
}
