<?php

namespace Tinderbox\Clickhouse;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\ClientException;
use Tinderbox\Clickhouse\Exceptions\ClusterException;
use Tinderbox\Clickhouse\Query\Mapper\NamedMapper;
use Tinderbox\Clickhouse\Transport\HttpTransport;

/**
 * @covers \Tinderbox\Clickhouse\Client
 * @use \Tinderbox\Clickhouse\Exceptions\ClientException
 * @use \Tinderbox\Clickhouse\Exceptions\ClusterException
 */
class ClientTest extends TestCase
{
    public function testClickhouseUsingServer()
    {
        $server = new Server('127.0.0.1');
        $client = new Client($server);

        $this->assertEquals($server, $client->getServer());

        $e = ClientException::clusterIsNotProvided();
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $client->using('h1');
    }

    public function testClickhouseUsingCluster()
    {
        $h1 = new Server('127.0.0.1');
        $h2 = new Server('127.0.0.2');
        $h3 = new Server('127.0.0.3');

        $cluster = new Cluster(
            [
                'h1' => $h1,
                'h2' => $h2,
                'h3' => $h3,
            ]
        );

        $client = new Client($cluster);

        $this->assertEquals($cluster, $client->getCluster());

        $this->assertEquals($h1, $client->getServer());

        $client->using('h2');

        $this->assertEquals($h2, $client->getServer());

        $client->removeCluster();

        $this->assertNull($client->getCluster());

        $client->setCluster($cluster);

        $e = ClusterException::serverNotFound('h4');
        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage($e->getMessage());

        $client->using('h4');
    }

    public function testClickhouseUsingWrongServer()
    {
        $wrongServer = null;

        $e = ClientException::invalidServerProvided($wrongServer);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $client = new Client($wrongServer);
    }

    protected function getMockedTransport(array $responses)
    {
        $mock = new MockHandler($responses);

        $handler = HandlerStack::create($mock);

        return new HttpTransport(new \GuzzleHttp\Client([
            'handler' => $handler,
        ]));
    }

    public function testClickhouseClientSyntaxError()
    {
        $transport = $transport = $this->getMockedTransport([
            new Response(500, [], 'Syntax error'),
        ]);

        $server = new Server('127.0.0.1');
        $client = new Client($server, null, $transport);

        $e = ClientException::serverReturnedError('Syntax error');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $client->select('seelect 1');
    }

    public function testClickhouseQueries()
    {
        $transport = $transport = $this->getMockedTransport([
            new Response(200, [], json_encode([
                'data' => [
                    [
                        '1' => 1,
                    ],
                ],
                'statistics' => [
                    'rows_read'  => 1,
                    'bytes_read' => 1,
                    'elapsed'    => 0.100,
                ],
            ])),

            new Response(200, [], json_encode([
                'data' => [
                    [
                        '1' => 1,
                    ],
                ],
                'statistics' => [
                    'rows_read'  => 1,
                    'bytes_read' => 1,
                    'elapsed'    => 0.100,
                ],
            ])),

            new Response(200, [], json_encode([
                'data' => [
                    [
                        '2' => 2,
                    ],
                ],
                'statistics' => [
                    'rows_read'  => 1,
                    'bytes_read' => 1,
                    'elapsed'    => 0.100,
                ],
            ])),

            new Response(200, [], json_encode([
                'data' => [
                    [
                        '3' => 3,
                    ],
                ],
                'statistics' => [
                    'rows_read'  => 1,
                    'bytes_read' => 1,
                    'elapsed'    => 0.100,
                ],
            ])),

            new Response(200, [], ''),
            new Response(200, [], ''),
            new Response(200, [], ''),
            new Response(200, [], ''),
            new Response(200, [], ''),
        ]);

        $server = new Server('127.0.0.1');
        $client = new Client($server, null, $transport);

        $result = $client->select('select 1');

        $this->assertEquals(1, $result[0]['1']);

        $this->assertEquals(1, $result->statistic->rows);
        $this->assertEquals(1, $result->statistic->bytes);
        $this->assertEquals(0.100, $result->statistic->time);

        $result = $client->selectAsync([
            ['select 1'],
            ['select 2'],
            ['select 3'],
        ]);

        $this->assertEquals(1, $result[0][0]['1']);
        $this->assertEquals(2, $result[1][0]['2']);
        $this->assertEquals(3, $result[2][0]['3']);

        $result = $client->statement('drop table test');

        $this->assertTrue($result);

        $result = $client->insert('insert into table (date, column) VALUES (\'2017-01-01\', 1)');

        $this->assertTrue($result);

        $files = [
            sys_get_temp_dir().'/test.csv',
            sys_get_temp_dir().'/test.csv',
            sys_get_temp_dir().'/test.csv',
        ];

        file_put_contents($files[0], '');

        $result = $client->insertFiles('table', ['column', 'column'], $files);

        $this->assertEquals(3, count($result));
        $this->assertTrue($result[0]);
        $this->assertTrue($result[1]);
        $this->assertTrue($result[2]);

        unlink($files[0]);

        $e = ClientException::insertFileNotFound($files[0]);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $client->insertFiles('table', ['column', 'column'], $files, 'csv');
    }

    public function testClickhouseMapper()
    {
        $mapper = new NamedMapper();

        $server = new Server('127.0.0.1');
        $client = new Client($server, $mapper);

        $this->assertInstanceOf(NamedMapper::class, $client->getMapper());
    }
}
