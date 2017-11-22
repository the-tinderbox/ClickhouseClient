<?php

namespace Tinderbox\Clickhouse\Query\Mapper;

use Tinderbox\Clickhouse\Exceptions\QueryMapperException;
use Tinderbox\Clickhouse\Interfaces\QueryMapperInterface;

/**
 * UnnamedMapper provides ability to use unnamed placeholders in query.
 *
 * Example:
 *
 * select * from table where column = ? and column = ?
 */
class UnnamedMapper extends AbstractMapper implements QueryMapperInterface
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
        $escapedBindings = $this->escapeBindings($bindings);

        $query = str_replace('%', '%%', $query);
        $query = str_replace('?', '%s', $query);

        $query = call_user_func_array('sprintf', array_merge([$query], $escapedBindings));

        return $query;
    }

    protected function getBindingPattern(): string
    {
        return '/\?/';
    }

    protected function checkBindingsPolicy(array $bindings)
    {
        $keys = array_keys($bindings);

        foreach ($keys as $key) {
            if (is_string($key)) {
                throw QueryMapperException::multipleBindingsType();
            }
        }
    }
}
