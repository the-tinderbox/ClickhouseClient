<?php

namespace Tinderbox\Clickhouse\Query;

use Tinderbox\Clickhouse\Exceptions\ResultException;
use Tinderbox\Clickhouse\Query;

/**
 * Query result.
 *
 * Container for request results and statistic
 */
class Result implements \ArrayAccess, \Iterator, \Countable
{
    /**
     * Query execution statistic.
     */
    protected QueryStatistic $statistic;

    /**
     * Result of query execution.
     */
    protected array $rows;

    /**
     * Query which was executed to get this result.
     */
    protected Query $query;

    /**
     * Current index for Iterator interface.
     */
    protected int $current = 0;

    /**
     * Result constructor.
     */
    public function __construct(Query $query, array $rows, QueryStatistic $statistic)
    {
        $this->setQuery($query);
        $this->setRows($rows);
        $this->setStatistic($statistic);
    }

    /**
     * Sets query.
     */
    protected function setQuery(Query $query): void
    {
        $this->query = $query;
    }

    /**
     * Sets statistic.
     */
    protected function setStatistic(QueryStatistic $statistic): void
    {
        $this->statistic = $statistic;
    }

    /**
     * Sets rows.
     */
    protected function setRows(array $rows): void
    {
        $this->rows = $rows;
    }

    /**
     * Returns rows.
     *
     * @return array
     */
    public function getRows(): array
    {
        return $this->rows;
    }

    /**
     * Returns query.
     *
     * @return Query
     */
    public function getQuery(): Query
    {
        return $this->query;
    }

    /**
     * Returns statistic.
     */
    public function getStatistic(): QueryStatistic
    {
        return $this->statistic;
    }

    /**
     * Getter to simplify access to rows and statistic.
     *
     * @throws ResultException
     */
    public function __get(string $name): mixed
    {
        $method = 'get'.ucfirst($name);

        if (method_exists($this, $method)) {
            return call_user_func([$this, $method]);
        }

        throw ResultException::propertyNotExists($name);
    }

    public function offsetExists($offset): bool
    {
        return isset($this->rows[$offset]);
    }

    public function offsetGet($offset): mixed
    {
        return $this->rows[$offset];
    }

    /**
     * @throws ResultException
     */
    public function offsetSet($offset, $value): void
    {
        throw ResultException::isReadonly();
    }

    /**
     * @throws ResultException
     */
    public function offsetUnset($offset): void
    {
        throw ResultException::isReadonly();
    }

    public function current(): mixed
    {
        return $this->rows[$this->current];
    }

    public function next(): void
    {
        $this->current++;
    }

    public function key(): mixed
    {
        return $this->current;
    }

    public function valid(): bool
    {
        return isset($this->rows[$this->current]);
    }

    public function rewind()
    {
        $this->current = 0;
    }

    public function count(): int
    {
        return count($this->rows);
    }
}
