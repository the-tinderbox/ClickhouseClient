<?php

namespace Tinderbox\Clickhouse\Interfaces;

use Tinderbox\Clickhouse\Query;
use Tinderbox\Clickhouse\Query\Result;

/**
 * Interface describes transport.
 */
interface TransportInterface
{
    /**
     * Executes queries which should not return result.
     *
     * Queries runs asyn
     *
     * @param Query[] $queries
     * @param int     $concurrency
     *
     * @return array
     */
    public function write(array $queries, int $concurrency = 5): array;

    /**
     * Executes queries which returns result of any select expression.
     *
     * @param array $queries
     * @param int   $concurrency
     *
     * @return Result[]
     */
    public function read(array $queries, int $concurrency = 5): array;
}
