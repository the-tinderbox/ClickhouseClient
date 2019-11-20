<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\FileFromString;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\ClusterException;
use Tinderbox\Clickhouse\Exceptions\ServerProviderException;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query\Mapper\NamedMapper;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Support\ServerTrait;
use Tinderbox\Clickhouse\Query\Meta;

/**
 * @covers \Tinderbox\Clickhouse\Client
 * @use \Tinderbox\Clickhouse\Exceptions\ClientException
 * @use \Tinderbox\Clickhouse\Exceptions\ClusterException
 */
class ClientTest extends TestCase
{
    use ServerTrait;

    public function testGetters()
    {
        $server = $this->getServer();
        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server);

        $client = new Client($serverProvider);

        $this->assertEquals(
            $serverProvider->getRandomServer(),
            $client->getServer(),
            'Correctly passes server provider'
        );
    }

    public function testMappers()
    {
        $server = $this->getServer();
        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server);

        $client = new Client($serverProvider, new NamedMapper());

        $result = $client->readOne('select number from numbers(:min, :max)', ['min' => 0, 'max' => 10], [], [], Format::TSV);

        $this->assertEquals(10, count($result->rows), 'Correctly changes mapper');
    }

    public function testTransports()
    {
        $server = $this->getServer();
        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server);

        $transport = $this->createMock(TransportInterface::class);
        $transport->method('read')->willReturn([
            new Result(new Query($server, ''), [0, 1], new QueryStatistic(0, 0, 0, 0), new Meta()),
        ]);

        $client = new Client($serverProvider, null, $transport);

        $result = $client->readOne('test query');

        $this->assertEquals(2, count($result->rows), 'Correctly changes transport');
    }

    public function testClusters()
    {
        $cluster = new Cluster(
            'test', [
                new Server('127.0.0.1'),
                new Server('127.0.0.2'),
                new Server('127.0.0.3'),
            ]
        );

        $cluster2 = new Cluster(
            'test2', [
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

    public function testClusterAndServersTogether()
    {
        $cluster = new Cluster(
            'test', [
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
        $serverProvider->addServer($this->getServer());

        return new Client($serverProvider);
    }

    public function testReadOne()
    {
        $client = $this->getClient();

        $result = $client->readOne('select * from numbers(?, ?) order by number desc format JSON', [0, 10]);

        $this->assertEquals(10, count($result->rows), 'Correctly executes query using mapper');
        $this->assertCount(1, $result->getMeta()->all());
    }

    public function testRead()
    {
        $client = $this->getClient();

        $result = $client->read(
            [
                [
                    'query'    => 'select * from numbers(?, ?) order by number desc',
                    'bindings' => [0, 10],
                    'format'   => Format::TSV,
                ],
                new Query($client->getServer(), 'select * from numbers(0, 20) order by number desc', [], [], Format::TSV),
                new Query($client->getServer(), 'select * from numbers(0, 20) where number in tab order by number desc', [
                    new TempTable('tab', new FileFromString('1'.PHP_EOL.'2'.PHP_EOL), ['number' => 'UInt64'], Format::TSV),
                ], [], Format::TSV),
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

        $client->writeOne('insert into default.tdchc_test_table (number) FORMAT TSV', [], [
            new FileFromString('1'.PHP_EOL.'2'.PHP_EOL),
        ]);

        $result = $client->readOne('select * from default.tdchc_test_table', [], [], [], Format::TSV);

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

        $result = $client->readOne('select * from default.tdchc_test_table', [], [], [], Format::TSV);

        $this->assertEquals(6, count($result->rows), 'Correctly writes data');
    }
}
