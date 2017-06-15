<?php

namespace Tinderbox\Clickhouse\Interfaces;

/**
 * Query Mapper interface.
 */
interface QueryMapperInterface
{
    /**
     * Binds values to query.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return string
     */
    public function bind(string $query, array $bindings): string;
}
