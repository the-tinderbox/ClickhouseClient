<?php

namespace Tinderbox\Clickhouse\Query;

use Tinderbox\Clickhouse\Exceptions\QueryStatisticException;

/**
 * Query statistic contains three metrics:.
 *
 * 1. rows - number of rows read by server
 * 2. bytes - number of bytes read by server
 * 3. time - query execution time in seconds
 */
class QueryStatistic
{
    /**
     * Number of rows read.
     */
    protected int $rows;

    /**
     * Number of bytes read.
     */
    protected int $bytes;

    /**
     * Query execution time in seconds.
     */
    protected float $time;

    /**
     * Rows before limit at least.
     */
    protected ?int $rowsBeforeLimitAtLeast;

    /**
     * QueryStatistic constructor.
     */
    public function __construct(int $rows, int $bytes, float $time, ?int $rowsBeforeLimitAtLeast = null)
    {
        $this->rows = $rows;
        $this->bytes = $bytes;
        $this->time = $time;
        $this->rowsBeforeLimitAtLeast = $rowsBeforeLimitAtLeast;
    }

    /**
     * Returns number of read rows.
     */
    public function getRows(): int
    {
        return $this->rows;
    }

    /**
     * Returns number of read bytes.
     */
    public function getBytes(): int
    {
        return $this->bytes;
    }

    /**
     * Returns query execution time.
     */
    public function getTime(): float
    {
        return $this->time;
    }

    /**
     * Returns rows before limit at least.
     */
    public function getRowsBeforeLimitAtLeast(): ?int
    {
        return $this->rowsBeforeLimitAtLeast;
    }

    /**
     * Getter to simplify access to rows, bytes and time.
     *
     * @throws QueryStatisticException
     */
    public function __get(string $name): mixed
    {
        $method = 'get'.ucfirst($name);

        if (method_exists($this, $method)) {
            return call_user_func([$this, $method]);
        }

        throw QueryStatisticException::propertyNotExists($name);
    }
}
