<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\MergedFiles;

/**
 * @covers \Tinderbox\Clickhouse\Common\AbstractFile
 * @covers \Tinderbox\Clickhouse\Common\MergedFiles
 */
class MergeFilesStreamTest extends TestCase
{
    public function testFile()
    {
        $ccatFileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $ccatFileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
            file_put_contents($fileName, $i.PHP_EOL);

            $ccatFileContent[] = $fileName;

            $result[] = $i.PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($ccatFileName, implode(PHP_EOL, $ccatFileContent));

        $file = new MergedFiles($ccatFileName);
        $stream = $file->open(false);

        $this->assertEquals($result, $stream->getContents(), 'Correctly reads content from ccat without encoding');

        unlink($ccatFileName);

        foreach ($ccatFileContent as $ccatFile) {
            unlink($ccatFile);
        }
    }

    public function testFileWithGzip()
    {
        $ccatFileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $ccatFileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
            file_put_contents($fileName, $i.PHP_EOL);

            $ccatFileContent[] = $fileName;

            $result[] = $i.PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($ccatFileName, implode(PHP_EOL, $ccatFileContent));

        $file = new MergedFiles($ccatFileName);
        $stream = $file->open();

        $this->assertEquals($result, gzdecode($stream->getContents()), 'Correctly reads content from ccat with encoding');

        unlink($ccatFileName);

        foreach ($ccatFileContent as $ccatFile) {
            unlink($ccatFile);
        }
    }
}
