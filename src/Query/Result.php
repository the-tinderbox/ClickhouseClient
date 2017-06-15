<?php

namespace Tinderbox\Clickhouse\Query;

use Tinderbox\Clickhouse\Exceptions\ResultException;

/**
 * Query result.
 *
 * Container for request results and statistic
 *
 * @property array          $rows
 * @property QueryStatistic $statistic
 */
class Result implements \ArrayAccess, \Iterator, \Countable
{
    /**
     * Query execution statistic.
     *
     * @var \Tinderbox\Clickhouse\Query\QueryStatistic
     */
    protected $statistic;

    /**
     * Result of query execution.
     *
     * @var array
     */
    protected $rows;

    /**
     * Current index for Iterator interface.
     *
     * @var int
     */
    protected $current = 0;

    /**
     * Result constructor.
     *
     * @param array                                      $rows
     * @param \Tinderbox\Clickhouse\Query\QueryStatistic $statistic
     */
    public function __construct(array $rows, QueryStatistic $statistic)
    {
        $this->setRows($rows);
        $this->setStatistic($statistic);
    }

    /**
     * Sets statistic.
     *
     * @param \Tinderbox\Clickhouse\Query\QueryStatistic $statistic
     */
    protected function setStatistic(QueryStatistic $statistic)
    {
        $this->statistic = $statistic;
    }

    /**
     * Sets rows.
     *
     * @param array $rows
     */
    protected function setRows(array $rows)
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
     * Returns statistic.
     *
     * @return \Tinderbox\Clickhouse\Query\QueryStatistic
     */
    public function getStatistic(): QueryStatistic
    {
        return $this->statistic;
    }

    /**
     * Getter to simplify access to rows and statistic.
     *
     * @param string $name
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ResultException
     *
     * @return mixed
     */
    public function __get($name)
    {
        $method = 'get'.ucfirst($name);

        if (method_exists($this, $method)) {
            return call_user_func([$this, $method]);
        }

        throw ResultException::propertyNotExists($name);
    }

    /*
     * ArrayAccess
     */

    public function offsetExists($offset)
    {
        return isset($this->rows[$offset]);
    }

    public function offsetGet($offset)
    {
        return $this->rows[$offset];
    }

    public function offsetSet($offset, $value)
    {
        throw ResultException::isReadonly();
    }

    public function offsetUnset($offset)
    {
        throw ResultException::isReadonly();
    }

    /*
     * Iterator
     */

    public function current()
    {
        return $this->rows[$this->current];
    }

    public function next()
    {
        ++$this->current;
    }

    public function key()
    {
        return $this->current;
    }

    public function valid()
    {
        return isset($this->rows[$this->current]);
    }

    public function rewind()
    {
        $this->current = 0;
    }

    /*
     * Countable
     */

    public function count()
    {
        return count($this->rows);
    }
}
