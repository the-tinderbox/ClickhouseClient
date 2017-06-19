<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\QueryMapperException;
use Tinderbox\Clickhouse\Query\Mapper\NamedMapper;
use Tinderbox\Clickhouse\Query\Mapper\UnnamedMapper;

/**
 * @covers \Tinderbox\Clickhouse\Query\Mapper\NamedMapper
 * @covers \Tinderbox\Clickhouse\Query\Mapper\UnnamedMapper
 */
class QueryMapperTest extends TestCase
{
    public function testUnnamedMapperCheckMultipleBindings()
    {
        $mapper = new UnnamedMapper();
        $sql = 'SELECT * FROM table WHERE column = ? AND column2 IN (?, ?, ?, ?, ?)';
        $bindigns = [1, 2, 3, 4, 5, 'named' => 'test'];

        $e = QueryMapperException::multipleBindingsType();
        $this->expectException(QueryMapperException::class);
        $this->expectExceptionMessage($e->getMessage());

        $mapper->bind($sql, $bindigns);
    }

    public function testUnnamedNumericBindings()
    {
        $mapper = new UnnamedMapper();
        $sql = 'SELECT * FROM table WHERE column = ? AND column2 IN (?, ?, ?, ?, ?)';
        $bindigns = [1, 2, 3, 4, 5, 6];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals('SELECT * FROM table WHERE column = 1 AND column2 IN (2, 3, 4, 5, 6)', $result);
    }

    public function testUnnamedStringBindings()
    {
        $mapper = new UnnamedMapper();
        $sql = 'SELECT * FROM table WHERE column = ? AND column2 IN (?, ?, ?, ?, ?)';
        $bindigns = ['test', 'a', 'b', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindigns = ["test with 'quotes' and / other \\bad stuff", 'a', 'b', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with \'quotes\' and / other \\\\bad stuff' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindigns = ['test with ? mark', 'a', 'b and here ? too', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with ? mark' AND column2 IN ('a', 'b and here ? too', 'c', 'd', 'e')", $result);

        $bindigns = ['test with %s text', 'a', 'b', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with %s text' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);
    }

    public function testNamedMapperCheckMultipleBindings()
    {
        $mapper = new NamedMapper();
        $sql = 'SELECT * FROM table WHERE column = :a AND column2 IN (:b, :c, :d, :e, :f)';

        $bindigns = [1, 2, 3, 4, 5, 'named' => 'test'];

        $e = QueryMapperException::multipleBindingsType();
        $this->expectException(QueryMapperException::class);
        $this->expectExceptionMessage($e->getMessage());

        $mapper->bind($sql, $bindigns);
    }

    public function testNamedNumericBindings()
    {
        $mapper = new NamedMapper();
        $sql = 'SELECT * FROM table WHERE column = :a AND column2 IN (:b, :c, :d, :e, :f)';
        $bindigns = [
            'a' => 1,
            'b' => 2,
            'c' => 3,
            'd' => 4,
            'e' => 5,
            'f' => 6,
        ];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals('SELECT * FROM table WHERE column = 1 AND column2 IN (2, 3, 4, 5, 6)', $result);
    }

    public function testNamedStringBindings()
    {
        $mapper = new NamedMapper();
        $sql = 'SELECT * FROM table WHERE column = :a AND column2 IN (:b, :c, :d, :e, :f)';
        $bindigns = [
            'a' => 'test',
            'b' => 'a',
            'c' => 'b',
            'd' => 'c',
            'e' => 'd',
            'f' => 'e',
        ];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindigns = [
            'a' => 'test with \'quotes\' and / other \\bad stuff',
            'b' => 'a',
            'c' => 'b',
            'd' => 'c',
            'e' => 'd',
            'f' => 'e',
        ];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with \'quotes\' and / other \\\\bad stuff' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindigns = [
            'a' => 'test with :name',
            'b' => 'a',
            'c' => 'b',
            'd' => 'c',
            'e' => 'd',
            'f' => 'e',
        ];

        $result = $mapper->bind($sql, $bindigns);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with :name' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);
    }
}
