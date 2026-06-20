<?php

declare(strict_types=1);

namespace BabelQueue\Examples\Outbox;

use BabelQueue\Outbox\OutboxRecord;
use BabelQueue\Outbox\OutboxStore;
use InitORM\Database\Database;

/**
 * A concrete {@see OutboxStore} backed by **InitORM** (`initorm/database`) — the thin
 * database adapter the framework-agnostic php-sdk deliberately does NOT ship (GR-7: the
 * core stays `ext-json`-only and binds to no DB driver). It lives here, in the examples
 * repo, so the SDK core has zero InitORM dependency.
 *
 * It implements the four-method contract over a single outbox table (see schema.sql):
 *
 *   - {@see save()}      INSERTs one encoded envelope. It runs whatever transaction is
 *                        *currently open* on the connection — it never begins/commits one
 *                        itself. The CALLER wraps its business write and this save in one
 *                        `$db->transaction(fn () => …)` so they commit atomically.
 *   - {@see fetchUnpublished()}  SELECTs pending rows, oldest first, capped by $limit.
 *   - {@see markPublished()}     UPDATEs the given rows to published.
 *   - {@see markFailed()}        bumps a row's attempt count and stores the last error.
 *
 * The envelope JSON is stored verbatim in the `body` column and read back unchanged, so
 * the bytes the relay publishes are byte-identical to what was written (GR-1/GR-5) and
 * `trace_id` is preserved end-to-end (GR-4).
 *
 * Every value is bound through InitORM's parameterised query builder (named placeholders);
 * no SQL is string-concatenated.
 */
final class InitOrmOutboxStore implements OutboxStore
{
    public function __construct(
        private readonly Database $db,
        private readonly string $table = 'babelqueue_outbox',
    ) {
    }

    /**
     * INSERT one encoded envelope as a pending row, **inside the caller's open transaction**.
     * Returns the row's auto-increment id as a string (the outbox id, not `meta.id`).
     */
    public function save(string $encodedEnvelope, string $queue): string
    {
        // NOTE: no begin/commit here — this participates in the transaction the caller
        // already opened around its business write (the whole point of the outbox).
        $this->db->create($this->table, [
            'queue' => $queue,
            'body' => $encodedEnvelope,
            'status' => 'pending',
            'attempts' => 0,
            'last_error' => null,
            'created_at' => $this->now(),
        ]);

        $id = $this->db->insertId();

        return $id === false ? '' : $id;
    }

    /**
     * SELECT up to $limit pending rows, oldest first. A production relay running multiple
     * workers would add `FOR UPDATE SKIP LOCKED` (or a `picked_at` claim) here so two
     * relays never grab the same row; at-least-once tolerates the rare overlap regardless.
     *
     * @return list<OutboxRecord>
     */
    public function fetchUnpublished(int $limit): array
    {
        $rows = $this->db
            ->from($this->table)
            ->where('status', '=', 'pending')
            ->orderBy('id', 'ASC')
            ->limit($limit)
            ->read()
            ->asAssoc()
            ->rows();

        $records = [];
        foreach ($rows as $row) {
            /** @var array<string, mixed> $row */
            $records[] = new OutboxRecord(
                (string) ($row['id'] ?? ''),
                is_string($row['body'] ?? null) ? $row['body'] : '',
                is_string($row['queue'] ?? null) ? $row['queue'] : 'default',
                is_numeric($row['attempts'] ?? null) ? (int) $row['attempts'] : 0,
            );
        }

        return $records;
    }

    /**
     * Mark the given outbox ids as published. Uses whereIn so it is one round-trip.
     *
     * @param  list<string>  $ids
     */
    public function markPublished(array $ids): void
    {
        if ($ids === []) {
            return;
        }

        $this->db
            ->from($this->table)
            ->whereIn('id', $ids)
            ->set('status', 'published')
            ->set('published_at', $this->now())
            ->update();
    }

    /**
     * Record a failed publish for one row: increment attempts, store the error, keep it
     * pending so a later relay pass retries it.
     */
    public function markFailed(string $id, string $error): void
    {
        $this->db
            ->from($this->table)
            ->where('id', '=', $id)
            ->set('attempts', $this->db->raw('attempts + 1'))
            ->set('last_error', $this->truncate($error))
            ->set('failed_at', $this->now())
            ->update();
    }

    private function now(): string
    {
        return gmdate('Y-m-d H:i:s');
    }

    /** Keep the stored error short; it is for operators, not a full stack trace. */
    private function truncate(string $error): string
    {
        return mb_substr($error, 0, 500);
    }
}
