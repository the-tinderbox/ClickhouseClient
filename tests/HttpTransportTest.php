<?php

namespace Tinderbox\Clickhouse;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\ClientException;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Transport\HttpTransport;

/**
 * @covers \Tinderbox\Clickhouse\Transport\HttpTransport
 */
class HttpTransportTest extends TestCase
{
    protected function getMockedTransport(array $responses)
    {
        $mock = new MockHandler($responses);

        $handler = HandlerStack::create($mock);

        return new HttpTransport(new \GuzzleHttp\Client([
            'handler' => $handler,
        ]));
    }

    protected function getServer() : Server
    {
        return new Server('127.0.0.1', '8123', 'default', 'user', 'pass');
    }

    public function testGetWithFile()
    {
        $file = sys_get_temp_dir().DIRECTORY_SEPARATOR.'numbers.csv';
        file_put_contents($file, '');

        $transport = $this->getMockedTransport([
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
        ]);

        $result = $transport->get($this->getServer(), 'select * from system.numbers where number in _numbers limit 6 format JSON', new TempTable('_numbers', $file, [
            'UInt64',
        ]));

        $this->assertInstanceOf(Result::class, $result);

        $result = $transport->get($this->getServer(), 'select * from system.numbers where number in _numbers or number in _numbers2 limit 6 format JSON', [
            new TempTable('_numbers', $file, [
                'numbers' => 'UInt64',
            ]),
            new TempTable('_numbers2', $file, [
                'UInt64',
            ]),
        ]);

        $this->assertInstanceOf(Result::class, $result);

        $result = $transport->getAsync($this->getServer(), [
            ['select * from system.numbers where number in _numbers or number in _numbers2 limit 6 format JSON', [
                new TempTable('_numbers', $file, [
                    'numbers' => 'UInt64',
                ]),
                new TempTable('_numbers2', $file, [
                    'UInt64',
                ]),
            ]],

            ['select * from system.numbers where number in _numbers or number in _numbers2 limit 6 format JSON', new TempTable('_numbers', $file, [
                'numbers' => 'UInt64',
            ])],
        ]);

        $this->assertInstanceOf(Result::class, $result[0]);
        $this->assertInstanceOf(Result::class, $result[1]);

        unlink($file);
    }

    public function testHttpTransportEmptyClient()
    {
        $transport = new HttpTransport();

        $e = ClientException::connectionError();
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $transport->get($this->getServer(), 'select 1');
    }

    public function testHttpTransportSyntaxError()
    {
        $server = $this->getServer();

        $transport = $this->getMockedTransport([
            new Response(500, [], 'Syntax error'),
        ]);

        $e = ClientException::serverReturnedError('Syntax error');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $transport->get($server, 'seelect 1');
    }

    public function testHttpTransportSendError()
    {
        $server = $this->getServer();

        $transport = $this->getMockedTransport([
            new Response(500, [], 'Syntax error'),
        ]);

        $e = ClientException::serverReturnedError('Syntax error');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $transport->send($server, 'inseert into table');
    }

    public function testHttpTransportMalformedResponse()
    {
        $server = $this->getServer();

        $transport = $this->getMockedTransport([
            new Response(200, [], 'some text'),
        ]);

        $e = ClientException::malformedResponseFromServer('some text');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $transport->get($server, 'select 1');
    }

    public function testHttpTransport()
    {
        $server = $this->getServer();

        $transport = $this->getMockedTransport([
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
        ]);

        $result = $transport->get($server, 'select 1');

        $this->assertEquals(1, $result[0]['1']);

        $this->assertEquals(1, $result->statistic->rows);
        $this->assertEquals(1, $result->statistic->bytes);
        $this->assertEquals(0.100, $result->statistic->time);

        $result = $transport->getAsync($server, [
            ['select 1'],
            ['select 2'],
            ['select 3'],
        ]);

        $this->assertEquals(1, $result[0][0]['1']);
        $this->assertEquals(2, $result[1][0]['2']);
        $this->assertEquals(3, $result[2][0]['3']);

        $result = $transport->send($server, 'insert into table (date, column) VALUES (\'2017-01-01\', 1)');

        $this->assertTrue($result);

        $files = [
            sys_get_temp_dir().'/test.csv',
            sys_get_temp_dir().'/test.csv',
            sys_get_temp_dir().'/test.csv',
        ];

        file_put_contents($files[0], '');

        $transport->sendAsyncFilesWithQuery($server, 'insert into table format csv', $files);

        unlink($files[0]);
    }

    public function testSendConnectionError()
    {
        $server = $this->getServer();

        $transport = $this->getMockedTransport([
            new ConnectException('Connection error', new Request('', '')),
        ]);

        $this->expectException(ClientException::class);

        $transport->send($server, 'select 1');
    }

    public function testGetAsyncConnectionError()
    {
        $server = $this->getServer();

        $transport = $this->getMockedTransport([
            new ConnectException('Connection error', new Request('', '')),
        ]);

        $this->expectException(ClientException::class);

        $transport->getAsync($server, [
            ['select 1'],
        ]);
    }

    public function testGetAsyncError()
    {
        $transport = $this->getMockedTransport([
            new Response(500, [], 'Syntax error'),
        ]);

        $e = ClientException::serverReturnedError('Syntax error');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($e->getMessage());

        $transport->getAsync($this->getServer(), [
            ['select 1'],
        ]);
    }

    public function testGetAsyncUnknownException()
    {
        $transport = $this->getMockedTransport([
            new \Exception('Unknown exception'),
        ]);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Unknown exception');

        $transport->getAsync($this->getServer(), [
            ['select 1'],
        ]);
    }
}
