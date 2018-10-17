<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\File;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;

/**
 * @covers \Tinderbox\Clickhouse\Common\AbstractFile
 * @covers \Tinderbox\Clickhouse\Common\TempTable
 */
class TempTableTest extends TestCase
{
    public function testGetters()
    {
        $file = new TempTable('name', '', ['number' => 'UInt64', 'string' => 'String'], Format::TSV);

        $this->assertEquals('name', $file->getName());
        $this->assertEquals(['number' => 'UInt64', 'string' => 'String'], $file->getStructure());
        $this->assertEquals(Format::TSV, $file->getFormat());
    }

    public function testFile()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i."\tsome".PHP_EOL;

            $result[] = $i."\tsome".PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($fileName, implode('', $fileContent));

        $file = new TempTable('name', $fileName, ['number' => 'UInt64', 'string' => 'String'], Format::TSV);
        $stream = $file->open(false);

        $this->assertEquals($result, $stream->getContents(), 'Correctly reads content from file without encoding');

        unlink($fileName);
    }

    public function testFileWithGzip()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i."\tsome".PHP_EOL;

            $result[] = $i."\tsome".PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($fileName, implode('', $fileContent));

        $file = new TempTable('name', $fileName, ['number' => 'UInt64', 'string' => 'String'], Format::TSV);
        $stream = $file->open();

        $this->assertEquals($result, gzdecode($stream->getContents()), 'Correctly reads content from file with encoding');

        unlink($fileName);
    }

    public function testWithFileInterfaceAsSource()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i.PHP_EOL;

            $result[] = $i.PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($fileName, implode('', $fileContent));

        $file = new File($fileName);
        $table = new TempTable('name', $file, ['number' => 'UInt64'], Format::TSV);

        $stream = $table->open(false);

        $this->assertEquals($result, $stream->getContents(), 'Correctly reads content from file without encoding');

        unlink($fileName);
    }

    public function testWithFileInterfaceAsSourceWithGzip()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i.PHP_EOL;

            $result[] = $i.PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($fileName, implode('', $fileContent));

        $file = new File($fileName);
        $table = new TempTable('name', $file, ['number' => 'UInt64'], Format::TSV);

        $stream = $table->open();

        $this->assertEquals($result, gzdecode($stream->getContents()), 'Correctly reads content from file with encoding');

        unlink($fileName);
    }
}
