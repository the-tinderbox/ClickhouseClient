<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\File;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Exceptions\ServerProviderException;

/**
 * @covers \Tinderbox\Clickhouse\ServerProvider
 */
class ServerProviderTest extends TestCase
{
    public function testClusters()
    {
        $servers = [
            new Server('127.0.0.1'),
            new Server('127.0.0.2'),
        ];

        $cluster = new Cluster('test_cluster', $servers);

        $provider = new ServerProvider();
        $provider->addCluster($cluster);

        $this->assertEquals($cluster, $provider->getCluster('test_cluster'), 'Correctly adds cluster');

        $firstServer = $provider->getRandomServerFromCluster('test_cluster');

        while ($firstServer == $provider->getRandomServerFromCluster('test_cluster')) {
            /* Randomize while get non used server */
        }

        $this->assertTrue(true, 'Correctly randomizes cluster servers');

        $this->assertEquals($servers[1], $provider->getServerFromCluster('test_cluster', '127.0.0.2'), 'Correctly returns server from cluster by hostname');
    }

    public function testClusterDuplicate()
    {
        $servers = [
            new Server('127.0.0.1'),
            new Server('127.0.0.2'),
        ];

        $cluster = new Cluster('test_cluster', $servers);

        $provider = new ServerProvider();
        $provider->addCluster($cluster);

        $this->expectException(ServerProviderException::class);
        $this->expectExceptionMessage('Can not add cluster with name [test_cluster], because it already added');

        $provider->addCluster($cluster);
    }

    public function testClusterNotFound()
    {
        $provider = new ServerProvider();

        $this->expectException(ServerProviderException::class);
        $this->expectExceptionMessage('Can not find cluster with name [test_cluster]');

        $provider->getCluster('test_cluster');
    }

    public function testServers()
    {
        $servers = [
            new Server('127.0.0.1'),
            new Server('127.0.0.2'),
        ];

        $provider = new ServerProvider();
        $provider->addServer($servers[0]);
        $provider->addServer($servers[1]);

        $this->assertEquals($servers[0], $provider->getServer('127.0.0.1'), 'Correctly adds server and returns it by hostname');

        $firstServer = $provider->getRandomServer();

        while ($firstServer === $provider->getRandomServer()) {
            /* Randomize while get non used server */
        }

        $this->assertTrue(true, 'Correctly randomizes servers');
    }

    public function testServerDuplicate()
    {
        $server = new Server('127.0.0.1');

        $provider = new ServerProvider();
        $provider->addServer($server);

        $this->expectException(ServerProviderException::class);
        $this->expectExceptionMessage('Server with hostname [127.0.0.1] already provided');

        $provider->addServer($server);
    }

    public function testServerNotFound()
    {
        $provider = new ServerProvider();

        $this->expectException(ServerProviderException::class);
        $this->expectExceptionMessage('Can not find server with hostname [127.0.0.1]');

        $provider->getServer('127.0.0.1');
    }
}
