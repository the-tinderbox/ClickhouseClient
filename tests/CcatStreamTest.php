<?php

namespace Tinderbox\Clickhouse;

use function GuzzleHttp\Psr7\stream_for;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Support\CcatStream;

/**
 * @covers \Tinderbox\Clickhouse\Support\CcatStream
 */
class CcatStreamTest extends TestCase
{
    public function testStreamSize()
    {
        $ccatStream = new CcatStream(stream_for('a'), '');

        $this->assertNull($ccatStream->getSize());
    }
}
