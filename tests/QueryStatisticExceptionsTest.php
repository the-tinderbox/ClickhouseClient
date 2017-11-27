<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\QueryStatisticException;

/**
 * @covers \Tinderbox\Clickhouse\Exceptions\QueryStatisticException
 */
class QueryStatisticExceptionsTest extends TestCase
{
    public function testPropertyNotExists(): void
    {
        $e = QueryStatisticException::propertyNotExists('test');

        $this->assertInstanceOf(QueryStatisticException::class, $e);
    }
}
