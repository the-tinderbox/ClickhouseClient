<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\ResultException;

/**
 * @covers \Tinderbox\Clickhouse\Exceptions\ResultException
 */
class ResultExceptionsTest extends TestCase
{
    public function testPropertyNotExists()
    {
        $e = ResultException::propertyNotExists('test');

        $this->assertInstanceOf(ResultException::class, $e);
    }

    public function testIsReadonly()
    {
        $e = ResultException::isReadonly();

        $this->assertInstanceOf(ResultException::class, $e);
    }
}
