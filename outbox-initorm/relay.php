<?php

/**
 * Relay — the PUBLISH side of the transactional outbox (ADR-0029).
 *
 * Run on a short interval (a worker loop, a scheduled command). It drains the durable
 * outbox rows the producer committed and forwards each onto the broker via a BabelQueue
 * Transport, then marks it published. At-least-once: if the process dies between publish
 * and markPublished, the row stays pending and is re-published next pass — a downstream
 * consumer dedupes on the canonical `meta.id` (the php-sdk's Idempotent::wrap, ADR-0022,
 * is the consumer-side mirror of this producer-side helper).
 *
 *   DB_DSN="sqlite:/tmp/babelqueue-outbox.sqlite" BROKER_URL="redis://localhost:6379/0" php relay.php
 *
 * The relay publishes the STORED BYTES verbatim — it never decodes or rebuilds the
 * envelope — so `trace_id` and every other field reach the broker byte-for-byte as
 * written (GR-1/GR-4/GR-5). Swap RedisTransport for any other BabelQueue transport.
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/bootstrap.php';

use BabelQueue\Examples\Outbox\InitOrmOutboxStore;
use BabelQueue\Outbox\OutboxRelay;
use BabelQueue\Transport\RedisTransport;
use Predis\Client;

/** @var InitORM\Database\Database $db */
$db = bootstrap_database();
bootstrap_schema($db);

$store = new InitOrmOutboxStore($db);

$brokerUrl = getenv('BROKER_URL') ?: 'redis://localhost:6379/0';
$transport = new RedisTransport(new Client($brokerUrl), 'orders');

$relay = new OutboxRelay($transport, $store, batchSize: 50);

$result = $relay->drain();

printf(
    "relay: published %d, failed %d (failed rows stay pending for the next run)\n",
    $result->published,
    $result->failed,
);
