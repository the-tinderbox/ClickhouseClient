<?php

namespace Tinderbox\Clickhouse;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\File;
use Tinderbox\Clickhouse\Common\FileFromString;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\MergedFiles;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\TransportException;
use Tinderbox\Clickhouse\Support\ServerTrait;
use Tinderbox\Clickhouse\Transport\HttpTransport;

/**
 * @covers \Tinderbox\Clickhouse\Transport\HttpTransport
 */
class HttpTransportTest extends TestCase
{
    use ServerTrait;

    protected function getMockedTransport(array $responses): HttpTransport
    {
        $mock = new MockHandler($responses);

        $handler = HandlerStack::create($mock);

        return new HttpTransport(new \GuzzleHttp\Client([
            'handler' => $handler,
        ]));
    }

    protected function getQuery(): Query
    {
        return new Query($this->getServer(), 'select * from table');
    }

    protected function getTransport(): HttpTransport
    {
        return new HttpTransport();
    }

    public function testRead()
    {
        $transport = $this->getMockedTransport([
            new Response(200, [], json_encode([
                'meta'                       => [
                    [
                        'name' => '1',
                        'type' => 'UInt32',
                    ]
                ],
                'data'                       => [
                    [
                        '1' => 1,
                    ],
                ],
                'statistics'                 => [
                    'rows_read'  => 1,
                    'bytes_read' => 1,
                    'elapsed'    => 0.100,
                ],
                'rows_before_limit_at_least' => 1024,
            ])),
        ]);

        $result = $transport->read([$this->getQuery()]);

        $this->assertEquals([
            [
                '1' => 1,
            ],
        ], $result[0]->rows, 'Returns correct response from server');

        $this->assertEquals([
            'rows_read'  => 1,
            'bytes_read' => 1,
            'elapsed'    => 0.100,
        ], [
            'rows_read'  => $result[0]->statistic->rows,
            'bytes_read' => $result[0]->statistic->bytes,
            'elapsed'    => $result[0]->statistic->time,
        ], 'Returns correct statistic from server');

        $this->assertCount(1, $result[0]->getMeta()->all());
        $metaColumn = $result[0]->getMeta()->getForColumn('1');
        $this->assertEquals(['1', 'UInt32'], [$metaColumn->getColumn(), $metaColumn->getType()], 'Parse meta correct');

        $this->assertEquals(1024, $result[0]->statistic->rowsBeforeLimitAtLeast, 'Returns correct rows_before_limit_at_least');
    }

    public function testReadMultipleRequests()
    {
        $transport = $this->getMockedTransport([
            new Response(200, [], json_encode([
                'meta'                       => [
                    [
                        'name' => '1',
                        'type' => 'UInt32',
                    ]
                ],
                'data'                       => [
                    [
                        '1' => 1,
                    ],
                ],
                'statistics'                 => [
                    'rows_read'  => 1,
                    'bytes_read' => 1,
                    'elapsed'    => 0.100,
                ],
                'rows_before_limit_at_least' => 1024,
            ])),

            new Response(200, [], json_encode([
                'meta'                       => [
                    [
                        'name' => '1',
                        'type' => 'Int32',
                    ]
                ],
                'data'                       => [
                    [
                        '1' => 2,
                    ],
                ],
                'statistics'                 => [
                    'rows_read'  => 2,
                    'bytes_read' => 2,
                    'elapsed'    => 0.100,
                ],
                'rows_before_limit_at_least' => 1025,
            ])),
        ]);

        $result = $transport->read([$this->getQuery(), $this->getQuery()]);

        $this->assertEquals([
            [
                '1' => 1,
            ],
        ], $result[0]->rows, 'Returns correct response from server');

        $this->assertEquals([
            'rows_read'  => 1,
            'bytes_read' => 1,
            'elapsed'    => 0.100,
        ], [
            'rows_read'  => $result[0]->statistic->rows,
            'bytes_read' => $result[0]->statistic->bytes,
            'elapsed'    => $result[0]->statistic->time,
        ], 'Returns correct statistic from server');

        $this->assertCount(1, $result[0]->getMeta()->all());
        $metaColumn = $result[0]->getMeta()->getForColumn('1');
        $this->assertEquals(['1', 'UInt32'], [$metaColumn->getColumn(), $metaColumn->getType()], 'Parse meta correct');

        $this->assertEquals(1024, $result[0]->statistic->rowsBeforeLimitAtLeast, 'Returns correct rows_before_limit_at_least');

        $this->assertEquals([
            [
                '1' => 1,
            ],
        ], $result[0]->rows, 'Returns correct response from server');

        $this->assertEquals([
            'rows_read'  => 2,
            'bytes_read' => 2,
            'elapsed'    => 0.100,
        ], [
            'rows_read'  => $result[1]->statistic->rows,
            'bytes_read' => $result[1]->statistic->bytes,
            'elapsed'    => $result[1]->statistic->time,
        ], 'Returns correct statistic from server');

        $this->assertCount(1, $result[1]->getMeta()->all());
        $metaColumn = $result[1]->getMeta()->getForColumn('1');
        $this->assertEquals(['1', 'Int32'], [$metaColumn->getColumn(), $metaColumn->getType()], 'Parse meta correct');

        $this->assertEquals(1025, $result[1]->statistic->rowsBeforeLimitAtLeast, 'Returns correct rows_before_limit_at_least');
    }

    public function testReadWithTablesUnstructured()
    {
        $transport = $this->getTransport();

        $tableSource = $this->getTempFileName();

        file_put_contents($tableSource, '1'.PHP_EOL.'2');

        $table = new TempTable('temp', $tableSource, ['UInt64'], Format::TSV);

        $query = new Query($this->getServer(), 'select * from numbers(0, 10) where number in temp FORMAT JSON', [$table]);
        $result = $transport->read([$query]);

        $this->assertEquals([1, 2], array_column($result[0]->rows, 'number'));

        unlink($tableSource);
    }

    public function testReadWithTablesStructured()
    {
        $transport = $this->getTransport();

        $tableSource = $this->getTempFileName();

        file_put_contents($tableSource, '1'.PHP_EOL.'2');

        $table = new TempTable('temp', $tableSource, ['number' => 'UInt64'], Format::TSV);

        $query = new Query($this->getServer(), 'select number from temp FORMAT JSON', [$table]);
        $result = $transport->read([$query]);

        $this->assertEquals([1, 2], array_column($result[0]->rows, 'number'), 'Returns correct result when uses temp tables for read queries');

        unlink($tableSource);
    }

    public function testReadWithTablesReject()
    {
        $transport = $this->getTransport();

        $query = new Query($this->getServer(), 'select * from temp2');

        $this->expectException(TransportException::class);

        $transport->read([$query]);
    }

    public function testWrite()
    {
        $transport = $this->getTransport();

        $query = new Query($this->getServer(), 'drop table if exists default.tdchc_test_table');
        $result = $transport->write([$query]);

        $this->assertTrue($result[0][0], 'Returns true on write queries without files');

        $query = new Query($this->getServer(), 'create table default.tdchc_test_table (number UInt64, string String) engine = Memory');
        $transport->write([$query]);

        $tableSource = $this->getTempFileName();

        file_put_contents($tableSource, "1\tsome".PHP_EOL."2\tstring");

        $table = new File($tableSource);

        $query = new Query($this->getServer(), 'insert into default.tdchc_test_table (number, string) FORMAT TSV', [$table]);
        $result = $transport->write([$query]);

        $this->assertTrue($result[0][0], 'Returns true on write queries');

        $query = new Query($this->getServer(), 'select * from default.tdchc_test_table order by number FORMAT JSON');
        $result = $transport->read([$query]);

        $this->assertEquals([
            [
                'number' => 1,
                'string' => 'some',
            ],
            [
                'number' => 2,
                'string' => 'string',
            ],
        ], $result[0]->rows, 'Returns correct result from recently created table and filled with temp files');

        $handle = $table->open();

        $query = new Query($this->getServer(), 'insert into default.tdchc_test_table (number, string) FORMAT TSV', [$table]);
        $result = $transport->write([$query]);

        $this->assertNotEquals($handle, $table->open(), 'Correctly closes file stream after write query');

        $query = new Query($this->getServer(), 'drop table if exists default.tdchc_test_table');
        $transport->write([$query]);

        unlink($tableSource);
    }

    public function testWriteMultipleFilesPerOneQuery()
    {
        $transport = $this->getTransport();

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table_2'),
        ]);

        $transport->write([
            new Query($this->getServer(), 'create table default.tdchc_test_table (number UInt64, string String) engine = Memory'),
            new Query($this->getServer(), 'create table default.tdchc_test_table_2 (number UInt64, string String) engine = Memory'),
        ]);

        $tableSource = $this->getTempFileName();
        $tableSource2 = $this->getTempFileName();

        file_put_contents($tableSource, "1\tsome".PHP_EOL."2\tstring");
        file_put_contents($tableSource2, "3\tstring".PHP_EOL."4\tsome");

        $table = new File($tableSource);
        $table2 = new File($tableSource2);

        $result = $transport->write([
            new Query($this->getServer(), 'insert into default.tdchc_test_table (number, string) FORMAT TSV', [$table, $table2]),
            new Query($this->getServer(), 'insert into default.tdchc_test_table_2 (number, string) FORMAT TSV', [$table2]),
        ]);

        $this->assertTrue($result[0][0] && $result[0][1] && $result[1][0], 'Returns true on multiple write queries with multiple files');

        $result = $transport->read([
            new Query($this->getServer(), 'select * from default.tdchc_test_table order by number FORMAT JSON'),
            new Query($this->getServer(), 'select * from default.tdchc_test_table_2 order by number FORMAT JSON'),
        ]);

        $this->assertEquals([
            [
                'number' => 1,
                'string' => 'some',
            ],
            [
                'number' => 2,
                'string' => 'string',
            ],
            [
                'number' => 3,
                'string' => 'string',
            ],
            [
                'number' => 4,
                'string' => 'some',
            ],
        ], $result[0]->rows, 'Returns correct result from recently created table and filled with temp files');

        $this->assertEquals([
            [
                'number' => 3,
                'string' => 'string',
            ],
            [
                'number' => 4,
                'string' => 'some',
            ],
        ], $result[1]->rows, 'Returns correct result from recently created table and filled with temp files');

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table_2'),
        ]);

        unlink($tableSource);
        unlink($tableSource2);
    }

    protected function getTempFileName(): string
    {
        return tempnam(sys_get_temp_dir(), 'tbchc_');
    }

    public function testConnectionError()
    {
        $transport = new HttpTransport(null, ['read' => ['connect_timeout' => 0.1]]);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Can\'t connect to the server [KHGIUYhakljsfnk:8123]');

        $transport->read([
            new Query(new Server('KHGIUYhakljsfnk'), ''),
        ]);
    }

    public function testUnknownReason()
    {
        $transport = $this->getMockedTransport([
            new \Exception('Unknown exception'),
        ]);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Unknown exception');

        $transport->read([
            new Query($this->getServer(), ''),
        ]);
    }

    public function testHttpTransportMalformedResponse()
    {
        $transport = $this->getMockedTransport([
            new Response(200, [], 'some text'),
        ]);

        $e = TransportException::malformedResponseFromServer('some text');
        $this->expectException(TransportException::class);
        $this->expectExceptionMessage($e->getMessage());

        $transport->read([
            new Query($this->getServer(), ''),
        ]);
    }

    public function testConnectionWithPassword()
    {
        $transport = $this->getTransport();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessageRegExp('/Wrong password for user default/');

        $server = $this->getServer('default', 'default', 'pass');
        $file = new TempTable('name', new FileFromString('aaa'), ['string' => 'String'], Format::TSV);

        $transport->read([new Query($server, 'select 1', [$file])]);
    }

    public function testConnectionWithPasswordOnWrite()
    {
        $transport = $this->getTransport();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessageRegExp('/Wrong password for user default/');

        $server = $this->getServer('default', 'default', 'pass');

        $transport->write([
            new Query($server, 'insert into table ', [new FileFromString('aaa')]),
        ]);
    }

    public function testFileInsert()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i.PHP_EOL;
        }

        file_put_contents($fileName, implode('', $fileContent));

        $file = new File($fileName);

        $transport = $this->getTransport();

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
        ]);

        $transport->write([
            new Query($this->getServer(), 'create table default.tdchc_test_table (number UInt64) engine = MergeTree order by number'),
        ]);

        $transport->write([
            new Query($this->getServer(), 'insert into default.tdchc_test_table (number) FORMAT TSV', [$file]),
        ]);

        $result = $transport->read([
            new Query($this->getServer(), 'select count() from default.tdchc_test_table FORMAT JSON'),
        ]);

        $this->assertEquals(100, $result[0]->rows[0]['count()'], 'File content may be used in insert statements');

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
        ]);
    }

    public function testMergedFilesInsert()
    {
        $ccatFileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $ccatFileContent = [];

        for ($i = 0; $i < 100; $i++) {
            $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
            file_put_contents($fileName, $i.PHP_EOL);

            $ccatFileContent[] = $fileName;
        }

        file_put_contents($ccatFileName, implode(PHP_EOL, $ccatFileContent));

        $file = new MergedFiles($ccatFileName);

        $transport = $this->getTransport();

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
        ]);

        $transport->write([
            new Query($this->getServer(), 'create table default.tdchc_test_table (number UInt64) engine = MergeTree order by number'),
        ]);

        $transport->write([
            new Query($this->getServer(), 'insert into default.tdchc_test_table (number) FORMAT TSV', [$file]),
        ]);

        $result = $transport->read([
            new Query($this->getServer(), 'select count() from default.tdchc_test_table FORMAT JSON'),
        ]);

        $this->assertEquals(100, $result[0]->rows[0]['count()'], 'File content may be used in insert statements');

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
        ]);
    }

    public function testTempTableReadAndWrite()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i."\t".($i >= 50 ? 'string' : 'some').PHP_EOL;
        }

        file_put_contents($fileName, implode('', $fileContent));

        $file = new TempTable('name', $fileName, ['number' => 'UInt64', 'string' => 'String'], Format::TSV);

        $transport = $this->getTransport();

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
        ]);

        $transport->write([
            new Query($this->getServer(), 'create table default.tdchc_test_table (number UInt64, string String) engine = MergeTree order by number'),
        ]);

        $transport->write([
            new Query($this->getServer(), 'insert into default.tdchc_test_table (number, string) FORMAT TSV', [$file]),
        ]);

        $result = $transport->read([
            new Query($this->getServer(), 'select string, count() from default.tdchc_test_table group by string FORMAT JSON'),
        ]);

        $this->assertTrue($result[0]->rows[0]['count()'] == 50 && $result[0]->rows[1]['count()'] == 50, 'File content may be used in insert statements');

        $result = $transport->read([
            new Query($this->getServer(), 'select string, count() from name group by string FORMAT JSON', [$file]),
        ]);

        $this->assertTrue($result[0]->rows[0]['count()'] == 50 && $result[0]->rows[1]['count()'] == 50, 'File content may be used in read statements');

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
        ]);
    }

    public function testFileFromStringReadAndWrite()
    {
        $fileContent = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i."\t".($i >= 50 ? 'string' : 'some').PHP_EOL;
        }

        $file = new FileFromString(implode('', $fileContent));
        $table = new TempTable('name', $file, ['number' => 'UInt64', 'string' => 'String'], Format::TSV);

        $transport = $this->getTransport();

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
            new Query($this->getServer(), 'create table default.tdchc_test_table (number UInt64, string String) engine = MergeTree order by number'),
            new Query($this->getServer(), 'insert into default.tdchc_test_table (number, string) FORMAT TSV', [$file]),
        ]);

        $result = $transport->read([
            new Query($this->getServer(), 'select string, count() from default.tdchc_test_table group by string FORMAT JSON'),
            new Query($this->getServer(), 'select string, count() from name group by string FORMAT JSON', [$table]),
        ]);

        $this->assertTrue($result[0]->rows[0]['count()'] == 50 && $result[0]->rows[1]['count()'] == 50, 'File content may be used in insert statements');
        $this->assertTrue($result[1]->rows[0]['count()'] == 50 && $result[1]->rows[1]['count()'] == 50, 'File content may be used in read statements');

        $transport->write([
            new Query($this->getServer(), 'drop table if exists default.tdchc_test_table'),
        ]);
    }

    public function testReadUnsupportedFormat()
    {
        $this->expectException(TransportException::class);
        $this->expectExceptionMessageRegExp('/Unsupported format/');

        $transport = $this->getTransport();
        $transport->read([
            new Query($this->getServer(), 'SELECT * FROM test', [], [], 'UNKNOWN'),
        ], 1);
    }

    public function testReadJsonCompact()
    {
        $fileContent = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = [$i, ($i >= 50 ? 'string' : 'some')];
        }

        $file = new FileFromString(implode(PHP_EOL, array_map(function (array $data) {
            return implode("\t", $data);
        }, $fileContent)));
        $table = new TempTable('name', $file, ['number' => 'UInt64', 'string' => 'String'], Format::TSV);

        $transport = $this->getTransport();
        $result = $transport->read([
            new Query($this->getServer(), 'SELECT * FROM name FORMAT JSONCompact', [$table], [], Format::JSONCompact),
        ], 1);

        $result = $result[0];

        $this->assertEquals($fileContent, $result->getRows());
        $this->assertCount(2, $result->getMeta()->all());
        $number = $result->getMeta()->getForColumn('number');
        $this->assertEquals(['number', 'UInt64'], [$number->getColumn(), $number->getType()]);

        $string = $result->getMeta()->getForColumn('string');
        $this->assertEquals(['string', 'String'], [$string->getColumn(), $string->getType()]);
    }
}
