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
        $statistic = new QueryStatistic(100, 1024, 0.122);

        $this->assertEquals(100, $statistic->getRows());
        $this->assertEquals(1024, $statistic->getBytes());
        $this->assertEquals(0.122, $statistic->getTime());

        $this->assertEquals(100, $statistic->rows);
        $this->assertEquals(1024, $statistic->bytes);
        $this->assertEquals(0.122, $statistic->time);

        $e = QueryStatisticException::propertyNotExists('miss');
        $this->expectException(QueryStatisticException::class);
        $this->expectExceptionMessage($e->getMessage());

        $statistic->miss;
    }
}
