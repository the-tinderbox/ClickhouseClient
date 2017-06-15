<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\ResultException;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;

/**
 * @covers \Tinderbox\Clickhouse\Query\Result
 * @use \Tinderbox\Clickhouse\Query\QueryStatistic
 * @use \Tinderbox\Clickhouse\Exceptions\ResultException
 */
class ResultTest extends TestCase
{
    public function testResult()
    {
        $rows = [
            [
                'col' => 1,
            ],
            [
                'col' => 2,
            ],
            [
                'col' => 3,
            ],
            [
                'col' => 4,
            ],
            [
                'col' => 5,
            ],
        ];
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($rows, $statistic);

        $this->assertEquals($rows, $result->getRows());
        $this->assertEquals($statistic, $result->getStatistic());

        $this->assertEquals($rows, $result->rows);
        $this->assertEquals($statistic, $result->statistic);

        $e = ResultException::propertyNotExists('miss');
        $this->expectException(ResultException::class);
        $this->expectExceptionMessage($e->getMessage());

        $result->miss;
    }

    public function testResultCountable()
    {
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result(['', '', ''], $statistic);

        $this->assertEquals(3, count($result));
    }

    public function testResultArrayAccessSet()
    {
        $rows = [
            [
                'col' => 1,
            ],
            [
                'col' => 2,
            ],
            [
                'col' => 3,
            ],
            [
                'col' => 4,
            ],
            [
                'col' => 5,
            ],
        ];
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($rows, $statistic);

        $this->assertArrayHasKey(3, $result);
        $this->assertEquals($rows[2], $result[2]);

        $e = ResultException::isReadonly();
        $this->expectException(ResultException::class);
        $this->expectExceptionMessage($e->getMessage());

        $result[1] = 'test';
    }

    public function testResultArrayAccessUnset()
    {
        $rows = [
            [
                'col' => 1,
            ],
            [
                'col' => 2,
            ],
            [
                'col' => 3,
            ],
            [
                'col' => 4,
            ],
            [
                'col' => 5,
            ],
        ];
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($rows, $statistic);

        $e = ResultException::isReadonly();
        $this->expectException(ResultException::class);
        $this->expectExceptionMessage($e->getMessage());

        unset($result[1]);
    }

    public function testResultIterator()
    {
        $rows = [
            [
                'col' => 1,
            ],
            [
                'col' => 2,
            ],
            [
                'col' => 3,
            ],
            [
                'col' => 4,
            ],
            [
                'col' => 5,
            ],
        ];
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($rows, $statistic);

        $prev = null;

        foreach ($result as $i => $row) {
            $this->assertNotEquals($prev, $row);

            $prev = $row;
        }
    }
}
