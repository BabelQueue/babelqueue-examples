<?php

declare(strict_types=1);

/**
 * Laravel consumer — reads the Artemis "orders" address over STOMP with the babelqueue-artemis
 * driver and routes each message by URN.
 *
 * In a real Laravel app you add a connection to config/queue.php and run a worker:
 *
 *     // config/queue.php
 *     'babelqueue-artemis' => [
 *         'driver'   => 'babelqueue-artemis',
 *         'host'     => env('ARTEMIS_HOST', '127.0.0.1'),
 *         'port'     => env('ARTEMIS_STOMP_PORT', 61613),
 *         'username' => env('ARTEMIS_USER', 'artemis'),
 *         'password' => env('ARTEMIS_PASSWORD', 'artemis'),
 *         'queue'    => 'orders',
 *     ],
 *
 *     php artisan queue:work babelqueue-artemis   # routes by URN via config/babelqueue.php handlers
 *
 * This standalone script is the framework-minimal equivalent for the demo: it drives the same
 * driver (connector → queue → pop()/delete()) without a full app, so it runs with just Composer.
 */

require __DIR__ . '/vendor/autoload.php';

use BabelQueue\Queue\Connectors\BabelQueueArtemisConnector;
use Illuminate\Container\Container;

$queue = (new BabelQueueArtemisConnector())->connect([
    'host'         => getenv('ARTEMIS_HOST') ?: '127.0.0.1',
    'port'         => (int) (getenv('ARTEMIS_STOMP_PORT') ?: 61613),
    'queue'        => 'orders',
    'username'     => 'artemis',
    'password'     => 'artemis',
    'read_timeout' => 3,
]);
$queue->setContainer(new Container());
$queue->setConnectionName('babelqueue-artemis');

echo "[laravel] consuming 'orders' over STOMP (the babelqueue-artemis driver)...\n";

$seen = 0;
$empties = 0;
while ($seen < 4 && $empties < 3) {
    $job = $queue->pop('orders');
    if ($job === null) {
        $empties++;
        continue;
    }
    $empties = 0;
    $seen++;

    printf(
        "[laravel] %-26s data=%s  from lang=%s  trace=%s  attempts=%d\n",
        $job->getUrn(),
        json_encode($job->getData(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        (string) ($job->getMeta()['lang'] ?? '?'),
        $job->getTraceId(),
        $job->attempts(),
    );

    $job->delete(); // STOMP ACK (client-individual)
}

echo "[laravel] done — consumed {$seen} message(s), all ACKed.\n";
