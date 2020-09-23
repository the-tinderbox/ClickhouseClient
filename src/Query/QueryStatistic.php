<?php

namespace Tinderbox\Clickhouse\Query;

use Tinderbox\Clickhouse\Exceptions\QueryStatisticException;

/**
 * Query statistic contains three metrics:.
 *
 * 1. rows - number of rows read by server
 * 2. bytes - number of bytes read by server
 * 3. time - query execution time in seconds
 *
 * @property int   $rows
 * @property int   $bytes
 * @property float $time
 * @property int   $rowsBeforeLimitAtLeast
 */
class QueryStatistic
{
    /**
     * Number of rows read.
     *
     * @var int
     */
    protected $rows;

    /**
     * Number of bytes read.
     *
     * @var int
     */
    protected $bytes;

    /**
     * Query execution time in seconds.
     *
     * @var float
     */
    protected $time;

    /**
     * Rows before limit at least.
     *
     * @var null|int
     */
    protected $rowsBeforeLimitAtLeast;

    /**
     * QueryStatistic constructor.
     *
     * @param int      $rows
     * @param int      $bytes
     * @param float    $time
     * @param null|int $rowsBeforeLimitAtLeast
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
     *
     * @return int
     */
    public function getRows(): int
    {
        return $this->rows;
    }

    /**
     * Returns number of read bytes.
     *
     * @return int
     */
    public function getBytes(): int
    {
        return $this->bytes;
    }

    /**
     * Returns query execution time.
     *
     * @return float
     */
    public function getTime(): float
    {
        return $this->time;
    }

    /**
     * Returns rows before limit at least.
     *
     * @return int|null
     */
    public function getRowsBeforeLimitAtLeast(): ?int
    {
        return $this->rowsBeforeLimitAtLeast;
    }

    /**
     * Getter to simplify access to rows, bytes and time.
     *
     * @param string $name
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\QueryStatisticException
     *
     * @return mixed
     */
    public function __get($name)
    {
        $method = 'get'.ucfirst($name);

        if (method_exists($this, $method)) {
            return call_user_func([$this, $method]);
        }

        throw QueryStatisticException::propertyNotExists($name);
    }
}
