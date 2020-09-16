<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\FileFromString;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\ServerOptions;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\ClusterException;
use Tinderbox\Clickhouse\Exceptions\ServerProviderException;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;

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

        $this->assertEquals(
            $serverProvider->getRandomServer(),
            $client->getServer(),
            'Correctly passes server provider'
        );
    }

    public function testTransports()
    {
        $server = new Server('127.0.0.1');
        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server);

        $transport = $this->createMock(TransportInterface::class);
        $transport->method('read')->willReturn([
            new Result(new Query($server, ''), [0, 1], new QueryStatistic(0, 0, 0, 0)),
        ]);

        $client = new Client($serverProvider, $transport);

        $result = $client->readOne('test query');

        $this->assertEquals(2, count($result->rows), 'Correctly changes transport');
    }

    public function testClusters()
    {
        $cluster = new Cluster(
            'test',
            [
                new Server('127.0.0.1'),
                new Server('127.0.0.2'),
                new Server('127.0.0.3'),
            ]
        );

        $cluster2 = new Cluster(
            'test2',
            [
                new Server('127.0.0.4'),
                new Server('127.0.0.5'),
                new Server('127.0.0.6'),
            ]
        );

        $serverProvider = new ServerProvider();
        $serverProvider->addCluster($cluster)->addCluster($cluster2);

        $client = new Client($serverProvider);

        $server = $client->onCluster('test')->getServer(); /* will return random server from cluster */
        $this->assertContains(
            $server,
            $cluster->getServers(),
            'Correctly returns random server from specified cluster'
        );

        $this->assertEquals($server, $client->getServer(), 'Remembers firstly selected random server for next calls');

        $client->using('127.0.0.3');
        $server = $client->getServer();

        $this->assertEquals(
            '127.0.0.3',
            $server->getHost(),
            'Correctly returns specified server from specified cluster'
        );

        $server = $client->onCluster('test2')->getServer(); /* will return random server from cluster */
        $this->assertContains(
            $server,
            $cluster2->getServers(),
            'Correctly returns random server from specified cluster'
        );

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
        $this->assertTrue(
            in_array($server->getHost(), ['127.0.0.1', '127.0.0.2', '127.0.0.3'], true),
            'Correctly returns random server'
        );

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

    public function testServersWithTags()
    {
        $serverOptionsWithTag = (new ServerOptions())->addTag('tag');

        $serverWithTag = (new Server('127.0.0.1', 8123))->setOptions($serverOptionsWithTag);
        $serverWithoutTag = new Server('127.0.0.2', 8123);

        $serverProvider = new ServerProvider();
        $serverProvider->addServer($serverWithTag)->addServer($serverWithoutTag);

        $client = new Client($serverProvider);
        $client->usingServerWithTag('tag');

        $server = $client->getServer();

        $this->assertEquals('127.0.0.1', $server->getHost());
        $this->assertEquals(8123, $server->getPort());
    }

    public function testServersWithTagsOnCluster()
    {
        $serverOptionsWithTag = (new ServerOptions())->addTag('tag');

        $serverWithTag = (new Server('127.0.0.1', 8123))->setOptions($serverOptionsWithTag);
        $serverWithoutTag = new Server('127.0.0.2', 8123);

        $cluster = new Cluster(
            'test',
            [
                $serverWithTag,
                $serverWithoutTag,
            ]
        );

        $serverProvider = new ServerProvider();
        $serverProvider->addCluster($cluster);

        $client = new Client($serverProvider);
        $client->onCluster('test')->usingServerWithTag('tag');

        $server = $client->getServer();

        $this->assertEquals('127.0.0.1', $server->getHost());
        $this->assertEquals(8123, $server->getPort());
    }

    public function testClusterAndServersTogether()
    {
        $cluster = new Cluster(
            'test',
            [
                new Server('127.0.0.1'),
                new Server('127.0.0.2'),
                new Server('127.0.0.3'),
            ]
        );

        $server1 = new Server('127.0.0.4');
        $server2 = new Server('127.0.0.5');
        $server3 = new Server('127.0.0.6');

        $serverProvider = new ServerProvider();
        $serverProvider->addCluster($cluster)->addServer($server1)->addServer($server2)->addServer($server3);

        $client = new Client($serverProvider);

        $server = $client->getServer();
        $this->assertTrue(
            in_array($server->getHost(), ['127.0.0.4', '127.0.0.5', '127.0.0.6'], true),
            'Correctly returns random server not in cluster'
        );

        $this->assertEquals($server, $client->getServer(), 'Remembers firstly selected random server for next calls');

        $client->onCluster('test');

        $server = $client->onCluster('test')->getServer(); /* will return random server from cluster */
        $this->assertContains(
            $server,
            $cluster->getServers(),
            'Correctly returns random server from specified cluster'
        );

        $this->assertEquals($server, $client->getServer(), 'Remembers firstly selected random server for next calls');

        $server = $client->onCluster(null)->getServer();
        $this->assertTrue(
            in_array($server->getHost(), ['127.0.0.4', '127.0.0.5', '127.0.0.6'], true),
            'Correctly returns random server after disabling cluster mode'
        );
    }

    protected function getClient(): Client
    {
        $serverProvider = new ServerProvider();
        $serverProvider->addServer(new Server('127.0.0.1', '8123', 'default', 'default', ''));

        return new Client($serverProvider);
    }

    public function testReadOne()
    {
        $client = $this->getClient();

        $result = $client->readOne('select * from numbers(0, 10) order by number desc');

        $this->assertEquals(10, count($result->rows), 'Correctly executes query using mapper');
    }

    public function testRead()
    {
        $client = $this->getClient();

        $result = $client->read(
            [
                [
                    'query'      => 'select * from numbers(0, 10) order by number desc',
                ],
                new Query($client->getServer(), 'select * from numbers(0, 20) order by number desc'),
                new Query($client->getServer(), 'select * from numbers(0, 20) where number in tab order by number desc', [
                    new TempTable('tab', new FileFromString('1'.PHP_EOL.'2'.PHP_EOL), ['number' => 'UInt64'], Format::TSV),
                ]),
            ]
        );

        $this->assertEquals(10, count($result[0]->rows), 'Correctly converts query from array to query instance');
        $this->assertEquals(20, count($result[1]->rows), 'Correctly executes queries');
        $this->assertEquals(2, count($result[2]->rows), 'Correctly executes query with file');
    }

    public function testWrite()
    {
        $client = $this->getClient();

        $client->write([
            new Query($client->getServer(), 'drop table if exists default.tdchc_test_table'),
            new Query($client->getServer(), 'create table default.tdchc_test_table (number UInt64) engine = Memory'),
        ], 1);

        $client->writeOne('insert into default.tdchc_test_table (number) FORMAT TSV', [
            new FileFromString('1'.PHP_EOL.'2'.PHP_EOL),
        ]);

        $result = $client->readOne('select * from default.tdchc_test_table');

        $this->assertEquals(2, count($result->rows), 'Correctly writes data');
    }

    public function testWriteFiles()
    {
        $client = $this->getClient();

        $client->write([
            new Query($client->getServer(), 'drop table if exists default.tdchc_test_table'),
            new Query($client->getServer(), 'create table default.tdchc_test_table (number UInt64) engine = Memory'),
        ], 1);

        $client->writeFiles('default.tdchc_test_table', ['number'], [
            new FileFromString('1'.PHP_EOL.'2'.PHP_EOL),
            new FileFromString('3'.PHP_EOL.'4'.PHP_EOL),
            new FileFromString('5'.PHP_EOL.'6'.PHP_EOL),
        ], Format::TSV);

        $result = $client->readOne('select * from default.tdchc_test_table');

        $this->assertEquals(6, count($result->rows), 'Correctly writes data');
    }
}
