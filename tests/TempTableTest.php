<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;

/**
 * @covers \Tinderbox\Clickhouse\Common\TempTable
 */
class TempTableTest extends TestCase
{
    public function testTempTable()
    {
        $table = new TempTable('table', 'source', [
            'structure',
        ], Format::TSV);

        $this->assertEquals('table', $table->getName());
        $this->assertEquals('source', $table->getSource());
        $this->assertEquals([
            'structure',
        ], $table->getStructure());
        $this->assertEquals(Format::TSV, $table->getFormat());
    }
}
