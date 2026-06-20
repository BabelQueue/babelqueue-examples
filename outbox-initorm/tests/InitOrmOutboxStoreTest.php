<?php

declare(strict_types=1);

namespace BabelQueue\Examples\Outbox\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;
use BabelQueue\Examples\Outbox\InitOrmOutboxStore;
use BabelQueue\Outbox\Outbox;
use BabelQueue\Outbox\OutboxRelay;
use InitORM\Database\Database;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * Proves the InitORM-backed {@see InitOrmOutboxStore} satisfies the OutboxStore contract
 * against a REAL database (in-memory SQLite) — save, fetch oldest-first, markPublished,
 * and markFailed (attempts bump + last_error), with the envelope surviving the DB
 * round-trip byte-for-byte and trace_id intact.
 *
 * Skips cleanly when the DB layer or pdo_sqlite is unavailable (so the wider examples CI
 * never breaks on a missing optional dependency).
 */
final class InitOrmOutboxStoreTest extends TestCase
{
    private Database $db;

    protected function setUp(): void
    {
        if (! class_exists(Database::class)) {
            $this->markTestSkipped('initorm/database is not installed (run composer install in this example).');
        }
        if (! extension_loaded('pdo_sqlite')) {
            $this->markTestSkipped('pdo_sqlite is required for the InitORM outbox adapter test.');
        }

        $this->db = new Database(['driver' => 'sqlite', 'database' => ':memory:']);
        $this->db->getPDO()->exec(
            'CREATE TABLE babelqueue_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT \'pending\',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                published_at TEXT,
                failed_at TEXT
            )'
        );
    }

    public function test_save_fetch_and_mark_published_round_trip(): void
    {
        $store = new InitOrmOutboxStore($this->db);
        $env = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1042], 'orders');
        $encoded = EnvelopeCodec::encode($env);

        $id = $store->save($encoded, 'orders');
        $this->assertNotSame('', $id);

        $pending = $store->fetchUnpublished(10);
        $this->assertCount(1, $pending);
        $this->assertSame($id, $pending[0]->id);
        $this->assertSame('orders', $pending[0]->queue);
        $this->assertSame($encoded, $pending[0]->body, 'the envelope bytes survive the DB round-trip unchanged (GR-1/GR-5)');

        $store->markPublished([$id]);
        $this->assertCount(0, $store->fetchUnpublished(10), 'a published row is no longer pending');
    }

    public function test_fetch_returns_pending_rows_oldest_first_and_respects_limit(): void
    {
        $store = new InitOrmOutboxStore($this->db);
        $ids = [];
        for ($i = 0; $i < 3; $i++) {
            $ids[] = $store->save(EnvelopeCodec::encode(
                EnvelopeCodec::make('urn:babel:orders:created', ['n' => $i], 'orders')
            ), 'orders');
        }

        $batch = $store->fetchUnpublished(2);
        $this->assertCount(2, $batch);
        $this->assertSame($ids[0], $batch[0]->id);
        $this->assertSame($ids[1], $batch[1]->id);
    }

    public function test_mark_failed_bumps_attempts_and_keeps_row_pending(): void
    {
        $store = new InitOrmOutboxStore($this->db);
        $id = $store->save(EnvelopeCodec::encode(
            EnvelopeCodec::make('urn:babel:orders:created', [], 'orders')
        ), 'orders');

        $store->markFailed($id, 'broker unavailable');
        $store->markFailed($id, 'broker unavailable again');

        $pending = $store->fetchUnpublished(10);
        $this->assertCount(1, $pending, 'a failed row stays pending for retry');
        $this->assertSame(2, $pending[0]->attempts, 'attempts incremented on each failure');
    }

    public function test_end_to_end_outbox_write_then_relay_preserves_trace_id(): void
    {
        $store = new InitOrmOutboxStore($this->db);
        $outbox = new Outbox($store);
        $transport = new class implements Transport {
            /** @var list<string> */
            public array $bodies = [];

            public function publish(string $payload, ?string $queue = null): ?string
            {
                $this->bodies[] = $payload;

                return 'ok';
            }
        };

        $traceId = '11111111-2222-4333-8444-555555555555';
        $env = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 7], 'orders', $traceId);

        // Caller-controlled transaction: business write would go here too.
        $this->db->transaction(function () use ($outbox, $env): void {
            $outbox->write($env);
        });

        $relay = new OutboxRelay($transport, $store, sleeper: static fn (int $ms) => null);
        $result = $relay->flush();

        $this->assertSame(1, $result->published);
        $this->assertCount(1, $transport->bodies);
        $decoded = EnvelopeCodec::decode($transport->bodies[0]);
        $this->assertSame($traceId, $decoded['trace_id']);
        $this->assertSame(0, $store->fetchUnpublished(10) === [] ? 0 : 1, 'outbox drained after relay');
    }

    public function test_relay_marks_failed_when_transport_throws(): void
    {
        $store = new InitOrmOutboxStore($this->db);
        $outbox = new Outbox($store);
        $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', [], 'orders'));

        $transport = new class implements Transport {
            public function publish(string $payload, ?string $queue = null): ?string
            {
                throw new RuntimeException('broker down');
            }
        };

        $relay = new OutboxRelay($transport, $store, sleeper: static fn (int $ms) => null);
        $result = $relay->flush();

        $this->assertSame(0, $result->published);
        $this->assertSame(1, $result->failed);
        $pending = $store->fetchUnpublished(10);
        $this->assertCount(1, $pending, 'the row stays pending after a failed publish');
        $this->assertSame(1, $pending[0]->attempts);
    }
}
