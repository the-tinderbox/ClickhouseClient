<?php

namespace Tinderbox\Clickhouse\Interfaces;

use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Server;

/**
 * Interface describes transport.
 */
interface TransportInterface
{
    /**
     * Sends query to given $server.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return bool
     */
    public function send(Server $server, string $query): bool;

    /**
     * Sends async insert queries with given files.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param array                        $files
     * @param int                          $concurrency
     *
     * @return array
     */
    public function sendAsyncFilesWithQuery(Server $server, string $query, array $files, int $concurrency = 5): array;

    /**
     * Executes SELECT queries and returns result.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param TempTable|null               $tables
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return \Tinderbox\Clickhouse\Query\Result
     */
    public function get(Server $server, string $query, $tables = null): Result;

    /**
     * Executes async SELECT queries and returns result.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param array                        $queries
     * @param int                          $concurrency
     *
     * @return array
     */
    public function getAsync(Server $server, array $queries, int $concurrency = 5): array;
}
