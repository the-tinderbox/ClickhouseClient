<?php

namespace Tinderbox\Clickhouse;

use GuzzleHttp\Psr7\Utils;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Support\CcatStream;

/**
 * @covers \Tinderbox\Clickhouse\Support\CcatStream
 */
class CcatStreamTest extends TestCase
{
    public function testStreamSize()
    {
        $ccatStream = new CcatStream(Utils::streamFor('a'), '');

        $this->assertNull($ccatStream->getSize());
    }
}
