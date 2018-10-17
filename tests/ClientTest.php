<?php

namespace Tinderbox\Clickhouse;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use function GuzzleHttp\Psr7\stream_for;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\ClientException;
use Tinderbox\Clickhouse\Exceptions\ClusterException;
use Tinderbox\Clickhouse\Exceptions\ServerProviderException;
use Tinderbox\Clickhouse\Query\Mapper\NamedMapper;
use Tinderbox\Clickhouse\Transport\HttpTransport;

/**
 * @covers \Tinderbox\Clickhouse\Client
 * @use \Tinderbox\Clickhouse\Exceptions\ClientException
 * @use \Tinderbox\Clickhouse\Exceptions\ClusterException
 */
class ClientTest extends TestCase
{
    public function testGetters()
    {
        $server = new Server('127.0.0.1');
        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server);

        $client = new Client($serverProvider);

        $this->assertEquals($serverProvider->getRandomServer(), $client->getServer(), 'Correctly passes server provider');
    }

    public function testClusters()
    {
        $cluster = new Cluster('test', [
            new Server('127.0.0.1'),
            new Server('127.0.0.2'),
            new Server('127.0.0.3'),
        ]);

        $cluster2 = new Cluster('test2', [
            new Server('127.0.0.4'),
            new Server('127.0.0.5'),
            new Server('127.0.0.6'),
        ]);

        $serverProvider = new ServerProvider();
        $serverProvider->addCluster($cluster)->addCluster($cluster2);

        $client = new Client($serverProvider);

        $server = $client->onCluster('test')->getServer(); /* will return random server from cluster */
        $this->assertContains($server, $cluster->getServers(), 'Correctly returns random server from specified cluster');

        $this->assertEquals($server, $client->getServer(), 'Remembers firstly selected random server for next calls');

        $client->using('127.0.0.3');
        $server = $client->getServer();

        $this->assertEquals('127.0.0.3', $server->getHost(), 'Correctly returns specified server from specified cluster');

        $server = $client->onCluster('test2')->getServer(); /* will return random server from cluster */
        $this->assertContains($server, $cluster2->getServers(), 'Correctly returns random server from specified cluster');

        $client->usingRandomServer();
        $server = $client->getServer();

        while ($server === $client->getServer()) {
            /* Randomize while get non used server */
        }

        $this->assertTrue(true, 'Correctly randomizes cluster servers on each call');

        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage('Server with hostname [127.0.0.0] is not found in cluster');

        $client->onCluster('test')->using('127.0.0.0')->getServer();
    }

    public function testServers()
    {
        $server1 = new Server('127.0.0.1');
        $server2 = new Server('127.0.0.2');
        $server3 = new Server('127.0.0.3');

        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server1)->addServer($server2)->addServer($server3);

        $client = new Client($serverProvider);

        $server = $client->getServer();
        $this->assertTrue(in_array($server->getHost(), ['127.0.0.1', '127.0.0.2', '127.0.0.3'], true), 'Correctly returns random server');

        $this->assertEquals($server, $client->getServer(), 'Remembers firstly selected random server for next calls');

        $server = $client->using('127.0.0.3')->getServer();
        $this->assertEquals('127.0.0.3', $server->getHost(), 'Correctly returns specified server');

        $client->usingRandomServer();
        $server = $client->getServer();

        while ($server === $client->getServer()) {
            /* Randomize while get non used server */
        }

        $this->assertTrue(true, 'Correctly randomizes cluster servers on each call');

        $this->expectException(ServerProviderException::class);
        $this->expectExceptionMessage('Can not find server with hostname [127.0.0.0]');

        $client->using('127.0.0.0')->getServer();
    }

    public function testClusterAndServersTogether()
    {
        $cluster = new Cluster('test', [
            new Server('127.0.0.1'),
            new Server('127.0.0.2'),
            new Server('127.0.0.3'),
        ]);

        $server1 = new Server('127.0.0.4');
        $server2 = new Server('127.0.0.5');
        $server3 = new Server('127.0.0.6');

        $serverProvider = new ServerProvider();
        $serverProvider->addCluster($cluster)->addServer($server1)->addServer($server2)->addServer($server3);

        $client = new Client($serverProvider);

        $server = $client->getServer();
        $this->assertTrue(in_array($server->getHost(), ['127.0.0.4', '127.0.0.5', '127.0.0.6'], true), 'Correctly returns random server not in cluster');

        $this->assertEquals($server, $client->getServer(), 'Remembers firstly selected random server for next calls');

        $client->onCluster('test');

        $server = $client->onCluster('test')->getServer(); /* will return random server from cluster */
        $this->assertContains($server, $cluster->getServers(), 'Correctly returns random server from specified cluster');

        $this->assertEquals($server, $client->getServer(), 'Remembers firstly selected random server for next calls');

        $server = $client->onCluster(null)->getServer();
        $this->assertTrue(in_array($server->getHost(), ['127.0.0.4', '127.0.0.5', '127.0.0.6'], true), 'Correctly returns random server after disabling cluster mode');
    }

    protected function getClient() : Client
    {
        $serverProvider = new ServerProvider();
        $serverProvider->addServer(new Server('127.0.0.1', '8123', 'default', 'default', ''));

        return new Client($serverProvider);
    }

    public function testReadOne()
    {
        $client = $this->getClient();

        $result = $client->readOne('select * from numbers(?, ?) order by number desc', [0, 10]);

        $this->assertEquals(10, count($result->rows), 'Correctly executes query using mapper');
    }
}
