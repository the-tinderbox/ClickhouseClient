<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\ServerOptions;
use Tinderbox\Clickhouse\Exceptions\ClusterException;
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

    public function testServersWithTags()
    {
        $serverOptionsWithTag = (new ServerOptions())->addTag('tag');

        $serverWithTag = new Server('127.0.0.1', 8123, 'default', 'default', '', $serverOptionsWithTag);
        $serverWithoutTag = new Server('127.0.0.2', 8123);

        $provider = new ServerProvider();
        $provider->addServer($serverWithTag);
        $provider->addServer($serverWithoutTag);

        $server = $provider->getRandomServerWithTag('tag');
        $this->assertEquals($server->getHost(), $serverWithTag->getHost(), 'Correctly adds server with tag and returns it');
    }

    public function testServerTagNotFound()
    {
        $provider = new ServerProvider();

        $this->expectException(ServerProviderException::class);
        $this->expectExceptionMessage('Can not find servers with tag [tag]');

        $provider->getRandomServerWithTag('tag');
    }

    public function testClustersWithServersWithTag()
    {
        $serverOptionsWithTag = (new ServerOptions())->addTag('tag');

        $serverWithTag = new Server('127.0.0.1', 8123, 'default', 'default', '', $serverOptionsWithTag);
        $serverWithoutTag = new Server('127.0.0.2');

        $servers = [
            $serverWithTag,
            $serverWithoutTag,
        ];

        $cluster = new Cluster('test', $servers);

        $provider = new ServerProvider();
        $provider->addCluster($cluster);

        $this->assertEquals($serverWithTag, $provider->getRandomServerFromClusterByTag('test', 'tag'), 'Correctly returns server from cluster by tag');
    }

    public function testServerTagNotFoundInCluster()
    {
        $servers = [
            new Server('127.0.0.1'),
        ];
        $cluster = new Cluster('test', $servers);

        $provider = new ServerProvider();
        $provider->addCluster($cluster);

        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage('There are no servers with tag [tag] in cluster');

        $provider->getRandomServerFromClusterByTag('test', 'tag');
    }
}
