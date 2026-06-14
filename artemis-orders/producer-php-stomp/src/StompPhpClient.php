<?php

declare(strict_types=1);

namespace App;

use BabelQueue\Transport\StompClient;
use Stomp\StatefulStomp;
use Stomp\Transport\Message;

/**
 * One-line adapter binding BabelQueue's StompClient seam to the stomp-php client.
 * stomp-php escapes header values per STOMP 1.2 (colons -> \c), so the §7 headers ride safely.
 */
final class StompPhpClient implements StompClient
{
    public function __construct(private readonly StatefulStomp $stomp)
    {
    }

    public function send(string $destination, string $body, array $headers): void
    {
        $this->stomp->send($destination, new Message($body, $headers));
    }
}
