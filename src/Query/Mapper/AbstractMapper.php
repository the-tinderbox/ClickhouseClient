<?php

namespace Tinderbox\Clickhouse\Query\Mapper;

use Tinderbox\Clickhouse\Common\Sanitizer;
use Tinderbox\Clickhouse\Exceptions\QueryMapperException;

/**
 * Abstract values Mapper.
 */
abstract class AbstractMapper
{
    /**
     * Checks bindings count in query and $bindings array
     * Checks if multiple bindings types used.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\QueryMapperException
     */
    protected function checkBindings(string $query, array $bindings)
    {
        $bindingsCountInQuery = $this->countBindingsFromQuery($query);
        $bindingsCount = count($bindings);

        if ($bindingsCountInQuery !== $bindingsCount) {
            throw QueryMapperException::wrongBindingsNumber($bindingsCountInQuery, $bindingsCount);
        }

        $this->checkBindingsPolicy($bindings);
    }

    /**
     * Counts bindings in query by given pattern.
     *
     * @param string $query
     *
     * @return int
     */
    protected function countBindingsFromQuery(string $query)
    {
        preg_match_all($this->getBindingPattern(), $query, $matches);

        return count($matches[0] ?? []);
    }

    /**
     * Escapes values.
     *
     * @param array $bindings
     *
     * @return array
     */
    protected function escapeBindings(array $bindings): array
    {
        foreach ($bindings as $key => $value) {
            $bindings[$key] = Sanitizer::escape($value);
        }

        return $bindings;
    }

    /**
     * Should return pattern to count bindings in query.
     *
     * @return string
     */
    abstract protected function getBindingPattern(): string;

    /**
     * Should check bindings policy.
     *
     * This method used to detect multiple bindings type
     *
     * @param array $bindings
     *
     * @return mixed
     */
    abstract protected function checkBindingsPolicy(array $bindings);
}
