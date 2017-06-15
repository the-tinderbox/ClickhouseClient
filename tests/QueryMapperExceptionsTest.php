<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\QueryMapperException;

/**
 * @covers \Tinderbox\Clickhouse\Exceptions\QueryMapperException
 */
class QueryMapperExceptionsTest extends TestCase
{
    public function testWrongBindingsNumber()
    {
        $e = QueryMapperException::wrongBindingsNumber(5, 7);

        $this->assertInstanceOf(QueryMapperException::class, $e);
    }

    public function testMultipleBindingsType()
    {
        $e = QueryMapperException::multipleBindingsType();

        $this->assertInstanceOf(QueryMapperException::class, $e);
    }
}
