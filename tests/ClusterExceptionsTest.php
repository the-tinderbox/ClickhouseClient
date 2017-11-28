<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\ClusterException;

/**
 * @covers \Tinderbox\Clickhouse\Exceptions\ClusterException
 */
class ClusterExceptionsTest extends TestCase
{
    public function testMissingServerHostname(): void
    {
        $e = ClusterException::missingServerHostname();

        $this->assertInstanceOf(ClusterException::class, $e);
    }

    public function testServerHostnameDuplicate(): void
    {
        $e = ClusterException::serverHostnameDuplicate('host-1');

        $this->assertInstanceOf(ClusterException::class, $e);
    }

    public function testInvalidServerProvided(): void
    {
        $e = ClusterException::invalidServerProvided(null);

        $this->assertInstanceOf(ClusterException::class, $e);
    }

    public function testServerNotFound(): void
    {
        $e = ClusterException::serverNotFound('host-1');

        $this->assertInstanceOf(ClusterException::class, $e);
    }
}
