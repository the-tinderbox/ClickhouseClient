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
    public function testUnnamedMapperCheckMultipleBindings(): void
    {
        $mapper = new UnnamedMapper();
        $sql = 'SELECT * FROM table WHERE column = ? AND column2 IN (?, ?, ?, ?, ?)';
        $bindings = [1, 2, 3, 4, 5, 'named' => 'test'];

        $e = QueryMapperException::multipleBindingsType();
        $this->expectException(QueryMapperException::class);
        $this->expectExceptionMessage($e->getMessage());

        $mapper->bind($sql, $bindings);
    }

    public function testUnnamedNumericBindings(): void
    {
        $mapper = new UnnamedMapper();
        $sql = 'SELECT * FROM table WHERE column = ? AND column2 IN (?, ?, ?, ?, ?)';
        $bindings = [1, 2, 3, 4, 5, 6];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals('SELECT * FROM table WHERE column = 1 AND column2 IN (2, 3, 4, 5, 6)', $result);
    }

    public function testUnnamedStringBindings(): void
    {
        $mapper = new UnnamedMapper();
        $sql = 'SELECT * FROM table WHERE column = ? AND column2 IN (?, ?, ?, ?, ?)';
        $bindings = ['test', 'a', 'b', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindings = ["test with 'quotes' and / other \\bad stuff", 'a', 'b', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with \'quotes\' and / other \\\\bad stuff' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindings = ['test with ? mark', 'a', 'b and here ? too', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with ? mark' AND column2 IN ('a', 'b and here ? too', 'c', 'd', 'e')", $result);

        $bindings = ['test with %s text', 'a', 'b', 'c', 'd', 'e'];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with %s text' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);
    }

    public function testNamedMapperCheckMultipleBindings(): void
    {
        $mapper = new NamedMapper();
        $sql = 'SELECT * FROM table WHERE column = :a AND column2 IN (:b, :c, :d, :e, :f)';

        $bindings = [1, 2, 3, 4, 5, 'named' => 'test'];

        $e = QueryMapperException::multipleBindingsType();
        $this->expectException(QueryMapperException::class);
        $this->expectExceptionMessage($e->getMessage());

        $mapper->bind($sql, $bindings);
    }

    public function testNamedNumericBindings(): void
    {
        $mapper = new NamedMapper();
        $sql = 'SELECT * FROM table WHERE column = :a AND column2 IN (:b, :c, :d, :e, :f)';
        $bindings = [
            'a' => 1,
            'b' => 2,
            'c' => 3,
            'd' => 4,
            'e' => 5,
            'f' => 6,
        ];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals('SELECT * FROM table WHERE column = 1 AND column2 IN (2, 3, 4, 5, 6)', $result);
    }

    public function testNamedStringBindings(): void
    {
        $mapper = new NamedMapper();
        $sql = 'SELECT * FROM table WHERE column = :a AND column2 IN (:b, :c, :d, :e, :f)';
        $bindings = [
            'a' => 'test',
            'b' => 'a',
            'c' => 'b',
            'd' => 'c',
            'e' => 'd',
            'f' => 'e',
        ];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindings = [
            'a' => 'test with \'quotes\' and / other \\bad stuff',
            'b' => 'a',
            'c' => 'b',
            'd' => 'c',
            'e' => 'd',
            'f' => 'e',
        ];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with \'quotes\' and / other \\\\bad stuff' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);

        $bindings = [
            'a' => 'test with :name',
            'b' => 'a',
            'c' => 'b',
            'd' => 'c',
            'e' => 'd',
            'f' => 'e',
        ];

        $result = $mapper->bind($sql, $bindings);

        $this->assertEquals("SELECT * FROM table WHERE column = 'test with :name' AND column2 IN ('a', 'b', 'c', 'd', 'e')", $result);
    }
}
