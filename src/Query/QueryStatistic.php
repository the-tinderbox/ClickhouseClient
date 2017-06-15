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
     * QueryStatistic constructor.
     *
     * @param int   $rows
     * @param int   $bytes
     * @param float $time
     */
    public function __construct(int $rows, int $bytes, float $time)
    {
        $this->rows = $rows;
        $this->bytes = $bytes;
        $this->time = $time;
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
