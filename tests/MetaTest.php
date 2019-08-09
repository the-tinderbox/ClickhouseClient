<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Query\MetaColumn;
use Tinderbox\Clickhouse\Query\Meta;
use PHPUnit\Framework\TestCase;

class MetaTest extends TestCase
{
    public function testConstructorMassAssign()
    {
        $columns = [new MetaColumn('test', 'String'), new MetaColumn('test2', 'String')];
        $meta = new Meta($columns);

        $this->assertCount(2, $meta->all());
    }

    public function testPush()
    {
        $meta = new Meta();
        $meta->push(new MetaColumn('test', 'String'));
        $meta->push(new MetaColumn('test2', 'String'));

        $this->assertCount(2, $meta->all());
    }

    public function testPushDuplicate()
    {
        $meta = new Meta();
        $meta->push(new MetaColumn('test', 'String'));
        $meta->push(new MetaColumn('test', 'String'));

        $this->assertCount(1, $meta->all());
    }

    public function testGetForColumn()
    {
        $meta = new Meta();
        $meta->push(new MetaColumn('test', 'String'));
        $meta->push(new MetaColumn('test2', 'Int32'));

        $this->assertEquals('Int32', $meta->getForColumn('test2')->getType());
    }
}
