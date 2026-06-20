<?php

/**
 * Shared bootstrap for the outbox example: build an InitORM Database from DB_DSN and
 * ensure the `orders` + `babelqueue_outbox` tables exist. Defaults to a throwaway SQLite
 * file so the demo runs with no server.
 */

declare(strict_types=1);

use InitORM\Database\Database;

/**
 * Build an InitORM {@see Database} from the DB_DSN env var.
 *
 * Examples:
 *   sqlite:/tmp/babelqueue-outbox.sqlite
 *   mysql:host=127.0.0.1;port=3306;dbname=demo   (also set DB_USER / DB_PASS)
 */
function bootstrap_database(): Database
{
    $dsn = getenv('DB_DSN') ?: 'sqlite:' . sys_get_temp_dir() . '/babelqueue-outbox.sqlite';

    if (str_starts_with($dsn, 'sqlite:')) {
        return new Database([
            'driver' => 'sqlite',
            'database' => substr($dsn, strlen('sqlite:')),
        ]);
    }

    // mysql / pgsql: parse host;port;dbname out of the DSN tail.
    [$driver, $tail] = explode(':', $dsn, 2) + [1 => ''];
    $parts = [];
    foreach (explode(';', $tail) as $pair) {
        if (str_contains($pair, '=')) {
            [$k, $v] = explode('=', $pair, 2);
            $parts[$k] = $v;
        }
    }

    return new Database([
        'driver' => $driver,
        'host' => $parts['host'] ?? '127.0.0.1',
        'port' => isset($parts['port']) ? (int) $parts['port'] : 3306,
        'database' => $parts['dbname'] ?? '',
        'username' => getenv('DB_USER') ?: null,
        'password' => getenv('DB_PASS') ?: null,
    ]);
}

/**
 * Create the demo `orders` table and the `babelqueue_outbox` table if they do not exist.
 * Raw DDL via PDO (InitORM has no migration layer); see schema.sql for the production DDL.
 */
function bootstrap_schema(Database $db): void
{
    $pdo = $db->getPDO();

    $pdo->exec(
        'CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL,
            created_at TEXT NOT NULL
        )'
    );

    $pdo->exec(
        'CREATE TABLE IF NOT EXISTS babelqueue_outbox (
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

    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_outbox_pending ON babelqueue_outbox (status, id)');
}
