<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\ServerOptions;
use Tinderbox\Clickhouse\Exceptions\ClusterException;

/**
 * @covers \Tinderbox\Clickhouse\Cluster
 * @use \Tinderbox\Clickhouse\Exceptions\ClusterException
 * @use \Tinderbox\Clickhouse\Common\ServerOptions
 */
class ClusterTest extends TestCase
{
    public function testClusterConstructor()
    {
        $cluster = new Cluster();

        $this->assertEmpty($cluster->getServers());

        $e = ClusterException::serverNotFound('host');
        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage($e->getMessage());

        $cluster->getServerByHostname('host');

        $e = ClusterException::missingServerHostname();
        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage($e->getMessage());

        $cluster->addServers(
            [
                [],
                [],
            ]
        );
    }

    public function testClusterWrongHostname()
    {
        $servers = [
            [
                'host'     => '127.0.0.1',
                'port'     => '8122',
                'database' => 'default',
                'username' => 'user',
                'password' => 'pass',
            ],

            'h2' => [
                'host' => '127.0.0.2',
                'port' => '8123',
            ],

            'h3' => new Server('127.0.0.3'),
        ];

        $e = ClusterException::missingServerHostname();
        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage($e->getMessage());

        $cluster = new Cluster($servers);
    }

    public function testClusterServers()
    {
        $options = (new ServerOptions())->setTimeout(10);

        $servers = [
            'h1' => [
                'host'     => '127.0.0.1',
                'port'     => '8122',
                'database' => 'default',
                'username' => 'user',
                'password' => 'pass',
                'options'  => $options,
            ],

            'h2' => [
                'host' => '127.0.0.2',
                'port' => '8123',
            ],

            'h3' => new Server('127.0.0.3'),
        ];

        $cluster = new Cluster($servers);

        $this->assertSameSize($servers, $cluster->getServers());

        $server = $cluster->getServerByHostname('h1');

        $this->assertEquals('127.0.0.1', $server->getHost());
        $this->assertEquals('8122', $server->getPort());
        $this->assertEquals('default', $server->getDatabase());
        $this->assertEquals('user', $server->getUsername());
        $this->assertEquals('pass', $server->getPassword());
        $this->assertEquals($options, $server->getOptions());

        $server = $cluster->getServerByHostname('h3');

        $this->assertEquals('127.0.0.3', $server->getHost());

        $server = new Server('127.0.0.4');

        $cluster->addServer('h4', $server);

        $this->assertEquals(4, count($cluster->getServers()));

        $e = ClusterException::serverHostnameDuplicate('h1');
        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage($e->getMessage());

        $cluster->addServer('h1', $server);
    }
}
