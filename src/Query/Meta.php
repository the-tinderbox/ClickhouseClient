<?php

namespace Tinderbox\Clickhouse\Query;

class Meta
{
    /** @var MetaColumn[] */
    protected $items = [];

    /**
     * Meta constructor.
     * @param Meta[] $columns
     */
    public function __construct(array $columns = [])
    {
        foreach ($columns as $column) {
            $this->push($column);
        }
    }

    public function push(MetaColumn $column)
    {
        $this->items[$column->getColumn()] = $column;
    }

    public function all(): array
    {
        return $this->items;
    }

    public function getForColumn(string $column): ?MetaColumn
    {
        return $this->items[$column] ?: null;
    }
}
