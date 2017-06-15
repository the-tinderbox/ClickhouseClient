<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\ClusterException;

/**
 * @covers \Tinderbox\Clickhouse\Exceptions\ClusterException
 */
class ClusterExceptionsTest extends TestCase
{
    public function testMissingServerHostname()
    {
        $e = ClusterException::missingServerHostname();

        $this->assertInstanceOf(ClusterException::class, $e);
    }

    public function testServerHostnameDuplicate()
    {
        $e = ClusterException::serverHostnameDuplicate('host-1');

        $this->assertInstanceOf(ClusterException::class, $e);
    }

    public function testInvalidServerProvided()
    {
        $e = ClusterException::invalidServerProvided(null);

        $this->assertInstanceOf(ClusterException::class, $e);
    }

    public function testServerNotFound()
    {
        $e = ClusterException::serverNotFound('host-1');

        $this->assertInstanceOf(ClusterException::class, $e);
    }
}
