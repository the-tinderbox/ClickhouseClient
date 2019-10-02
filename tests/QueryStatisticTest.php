<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\QueryStatisticException;
use Tinderbox\Clickhouse\Query\QueryStatistic;

/**
 * @covers \Tinderbox\Clickhouse\Query\QueryStatistic
 */
class QueryStatisticTest extends TestCase
{
    public function testQueryStatistic()
    {
        $statistic = new QueryStatistic(100, 1024, 0.122, 1024);

        $this->assertEquals(100, $statistic->getRows(), 'Returns correct rows number passed in constructor');
        $this->assertEquals(1024, $statistic->getBytes(), 'Returns correct bytes passed in constructor');
        $this->assertEquals(0.122, $statistic->getTime(), 'Returns correct time passed in constructor');
        $this->assertEquals(1024, $statistic->getRowsBeforeLimitAtLeast(), 'Returns correct rows before limit at least passed in constructor');

        $this->assertEquals(100, $statistic->rows, 'Returns correct rows number passed in constructor via magic property');
        $this->assertEquals(1024, $statistic->bytes, 'Returns correct bytes passed in constructor via magic property');
        $this->assertEquals(0.122, $statistic->time, 'Returns correct time passed in constructor via magic property');
        $this->assertEquals(1024, $statistic->rowsBeforeLimitAtLeast, 'Returns correct rows before limit at least passed in constructor via magic property');

        $e = QueryStatisticException::propertyNotExists('miss');
        $this->expectException(QueryStatisticException::class);
        $this->expectExceptionMessage($e->getMessage());

        $statistic->miss;
    }
}
