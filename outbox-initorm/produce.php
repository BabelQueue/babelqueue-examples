<?php

/**
 * Producer — the WRITE side of the transactional outbox (ADR-0029).
 *
 * It demonstrates the one thing the pattern exists for: the business row and the
 * BabelQueue message are committed **atomically, in a single database transaction**, so
 * a crash can never leave the order saved without its `urn:babel:orders:created` message
 * (the classic dual-write bug). Nothing is published here — a separate relay (relay.php)
 * does that afterwards.
 *
 *   composer install
 *   DB_DSN="sqlite:/tmp/babelqueue-outbox.sqlite" php produce.php
 *
 * The transaction BOUNDARY is the caller's: we open one `$db->transaction(...)`, do the
 * business INSERT and the `$outbox->write()` inside it, and InitORM commits both together
 * (or rolls both back). The php-sdk's Outbox helper never begins/commits anything.
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/bootstrap.php';

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Examples\Outbox\InitOrmOutboxStore;
use BabelQueue\Outbox\Outbox;

/** @var InitORM\Database\Database $db */
$db = bootstrap_database();
bootstrap_schema($db);

$store = new InitOrmOutboxStore($db);
$outbox = new Outbox($store);

$orders = [
    ['order_id' => 1042, 'amount' => 99.90, 'currency' => 'USD'],
    ['order_id' => 1043, 'amount' => 12.50, 'currency' => 'EUR'],
    ['order_id' => 1044, 'amount' => 7.25, 'currency' => 'GBP'],
];

foreach ($orders as $order) {
    // ONE transaction for both writes. If either throws, BOTH roll back — the order is
    // never saved without its outbox message, and vice versa.
    $db->transaction(function (InitORM\Database\Database $db) use ($outbox, $order): void {
        // 1) the business write
        $db->create('orders', [
            'order_id' => $order['order_id'],
            'amount' => $order['amount'],
            'currency' => $order['currency'],
            'created_at' => gmdate('Y-m-d H:i:s'),
        ]);

        // 2) the message — encoded once via the FROZEN codec, stored verbatim in the
        //    same transaction. A fresh trace_id is minted at the origin (GR-4).
        $envelope = EnvelopeCodec::make('urn:babel:orders:created', $order, 'orders');
        $outbox->write($envelope);
    });

    printf("committed order #%d + its outbox message in one transaction\n", $order['order_id']);
}

$pending = $db->from('babelqueue_outbox')->where('status', '=', 'pending')->read()->numRows();
printf("\n%d message(s) sit in the outbox, durable and unpublished. Run: php relay.php\n", $pending);
