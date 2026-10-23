<?php

declare(strict_types=1);

/**
 * Interop probe (Laravel) for the R1 Redis scenarios.
 *
 * Drives the real `babelqueue-redis` connection (BabelQueueRedisConnector -> BabelQueueRedisQueue,
 * the Laravel zset layout) without booting a full application.
 *
 * Usage: php probe.php push|consume
 *
 * Environment (same contract as ../go/main.go):
 *   REDIS_URL    redis://localhost:6379/0
 *   QUEUE        queue name
 *   RETRY_AFTER  seconds (default 90)
 *   TIMEOUT      seconds before giving up (consume)
 *   COUNT        messages to push (push, default 1)
 *   FROM         label stored in data.from (push, default laravel)
 *   IDLE         consume: seconds without a message before exiting 0 (default 3)
 *   MAX          consume: stop after this many messages (default 0 = until idle)
 *   AUTOLOAD     path to a composer autoload.php (default ./vendor/autoload.php)
 *
 * Output lines mirror the other probes ("PROBE laravel ...").
 */

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Queue\Connectors\BabelQueueRedisConnector;
use Illuminate\Container\Container;
use Illuminate\Redis\RedisManager;

$autoload = getenv('AUTOLOAD') ?: __DIR__ . '/vendor/autoload.php';
if (! is_file($autoload)) {
    fwrite(STDERR, "laravel probe: autoload not found: {$autoload}\n");
    exit(2);
}
require $autoload;

function envOr(string $key, string $default): string
{
    $value = getenv($key);

    return ($value === false || $value === '') ? $default : $value;
}

$parts = parse_url(envOr('REDIS_URL', 'redis://localhost:6379/0')) ?: [];
$queueName = envOr('QUEUE', 'orders');
$retryAfter = (int) envOr('RETRY_AFTER', '90');
$command = $argv[1] ?? 'consume';

$container = new Container();
$redis = new RedisManager($container, 'predis', [
    'default' => [
        'host' => $parts['host'] ?? 'localhost',
        'port' => $parts['port'] ?? 6379,
        'database' => isset($parts['path']) && $parts['path'] !== '/' ? (int) ltrim($parts['path'], '/') : 0,
    ],
]);

/** @var \BabelQueue\Queue\BabelQueueRedisQueue $queue */
$queue = (new BabelQueueRedisConnector($redis))->connect([
    'connection' => 'default',
    'queue' => $queueName,
    'retry_after' => $retryAfter,
    'block_for' => null,
]);
$queue->setContainer($container);

$nowMs = static fn (): int => (int) floor(microtime(true) * 1000);

if ($command === 'push') {
    $count = (int) envOr('COUNT', '1');
    for ($i = 0; $i < $count; $i++) {
        $envelope = EnvelopeCodec::make(
            'urn:babel:orders:created',
            ['order_id' => 2001 + $i, 'from' => envOr('FROM', 'laravel')],
            $queueName,
        );
        $at = $nowMs();
        $queue->pushRaw(EnvelopeCodec::encode($envelope), $queueName);
        echo sprintf("PROBE laravel PUBLISHED id=%s at_ms=%d\n", $envelope['meta']['id'], $at);
    }
    exit(0);
}

if ($command !== 'consume') {
    fwrite(STDERR, "usage: probe.php push|consume\n");
    exit(2);
}

echo "PROBE laravel READY\n";
$deadline = microtime(true) + (float) envOr('TIMEOUT', '30');
$idle = (float) envOr('IDLE', '3');
$max = (int) envOr('MAX', '0');
$handled = 0;
$lastMessage = microtime(true);

while (microtime(true) < $deadline) {
    $job = $queue->pop($queueName);
    if ($job === null) {
        if ($handled > 0 || $max === 0) {
            if (microtime(true) - $lastMessage >= $idle) {
                echo "PROBE laravel DRAINED\n";
                exit(0);
            }
        }
        usleep(200_000);
        continue;
    }

    $lastMessage = microtime(true);
    $decoded = json_decode($job->getRawBody(), true);
    $id = (string) ($decoded['meta']['id'] ?? $job->getJobId());
    // BabelQueueJob::attempts() is the post-reserve count (envelope attempts + 1). NOTE: the Go, Node,
    // Python, Java and .NET probes print the RAW envelope `attempts` (the value before the reserve),
    // so this number is one higher than theirs for the same delivery; no scenario compares them.
    echo sprintf("PROBE laravel RECEIVED id=%s attempts=%d at_ms=%d\n", $id, $job->attempts(), $nowMs());
    $hold = (float) envOr('HOLD', '0');
    if ($hold > 0) {
        usleep((int) ($hold * 1_000_000)); // lets a concurrent consumer take a share of the queue
    }
    $job->delete();
    echo sprintf("PROBE laravel ACKED id=%s\n", $id);
    $handled++;
    if ($max > 0 && $handled >= $max) {
        exit(0);
    }
}

echo "PROBE laravel TIMEOUT\n";
exit(3);
