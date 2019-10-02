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
    public function testGetters()
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
        $query = new Query(new Server('localhost'), '');
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($query, $rows, $statistic);

        $this->assertEquals($rows, $result->getRows(), 'Returns rows passed to constructor');
        $this->assertEquals($statistic, $result->getStatistic(), 'Returns statistic passed to constructor');
        $this->assertEquals($query, $result->getQuery(), 'Returns query passed to constructor');

        $this->assertEquals($rows, $result->rows, 'Returns rows passed to constructor via magic property');
        $this->assertEquals($statistic, $result->statistic, 'Returns statistic passed to constructor via magic property');
        $this->assertEquals($query, $result->query, 'Returns query passed to constructor via magic property');

        $e = ResultException::propertyNotExists('miss');
        $this->expectException(ResultException::class);
        $this->expectExceptionMessage($e->getMessage());

        $result->miss;
    }

    public function testResultCountable()
    {
        $query = new Query(new Server('localhost'), '');
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($query, ['', '', ''], $statistic);

        $this->assertEquals(3, count($result), 'Returns correct rows count via Countable interface');
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
        $query = new Query(new Server('localhost'), '');
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($query, $rows, $statistic);

        $e = ResultException::isReadonly();
        $this->expectException(ResultException::class);
        $this->expectExceptionMessage($e->getMessage());

        $result[1] = 'test';
    }

    public function testResultArrayAccessGet()
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
        $query = new Query(new Server('localhost'), '');
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($query, $rows, $statistic);
        $this->assertTrue(isset($result[1]), 'Correctly determines that offset exists via ArrayAccess interface');
        $this->assertFalse(isset($result[10]), 'Correctly determines that offset does not exists via ArrayAccess interface');
        $this->assertEquals($rows[0], $result[0], 'Correctly returns offset via ArrayAccess interface');
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
        $query = new Query(new Server('localhost'), '');
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($query, $rows, $statistic);

        $e = ResultException::isReadonly();
        $this->expectException(ResultException::class);
        $this->expectExceptionMessage($e->getMessage());

        unset($result[1]);
    }

    public function testResultIterator()
    {
        $this->setName('Correctly iterates over rows via Iterator interface');

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
        $query = new Query(new Server('localhost'), '');
        $statistic = new QueryStatistic(5, 1024, 0.122);

        $result = new Result($query, $rows, $statistic);

        $prev = null;

        foreach ($result as $i => $row) {
            $this->assertNotEquals($prev, $row);

            $prev = $row;
        }
    }
}
