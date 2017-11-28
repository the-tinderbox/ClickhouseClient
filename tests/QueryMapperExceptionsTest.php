<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\QueryMapperException;

/**
 * @covers \Tinderbox\Clickhouse\Exceptions\QueryMapperException
 */
class QueryMapperExceptionsTest extends TestCase
{
    public function testMultipleBindingsType(): void
    {
        $e = QueryMapperException::multipleBindingsType();

        $this->assertInstanceOf(QueryMapperException::class, $e);
    }

    public function testWrongBindingsNumber(): void
    {
        $e = QueryMapperException::wrongBindingsNumber(2, 1);

        $this->assertInstanceOf(QueryMapperException::class, $e);
    }
}
