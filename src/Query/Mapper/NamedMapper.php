<?php

namespace Tinderbox\Clickhouse\Query\Mapper;

use Tinderbox\Clickhouse\Exceptions\QueryMapperException;
use Tinderbox\Clickhouse\Interfaces\QueryMapperInterface;

/**
 * NamedMapper provides ability to use named placeholders in query.
 *
 * Example:
 *
 * select * from table where column = :value and column = :value2
 */
class NamedMapper extends AbstractMapper implements QueryMapperInterface
{
    /**
     * Binds values to query.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return string
     */
    public function bind(string $query, array $bindings): string
    {
        $this->checkBindings($query, $bindings);
        $bindings = $this->escapeBindings($bindings);

        foreach ($bindings as $key => $value) {
            if (substr($key, 0, 1) !== ':') {
                $key = ':'.$key;
            }

            $query = str_replace($key, $value, $query);
        }

        return $query;
    }

    protected function getBindingPattern(): string
    {
        return '/:[a-zA-Z0-9]+/';
    }

    protected function checkBindingsPolicy(array $bindings)
    {
        $keys = array_keys($bindings);

        foreach ($keys as $key) {
            if (!is_string($key)) {
                throw QueryMapperException::multipleBindingsType();
            }
        }
    }
}
