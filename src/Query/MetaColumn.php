<?php

namespace Tinderbox\Clickhouse\Query;

class MetaColumn
{
    /**
     * @var string
     */
    protected $column;
    /**
     * @var string
     */
    protected $type;

    public function __construct(string $column, string $type)
    {
        $this->column = $column;
        $this->type = $type;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function getColumn(): string
    {
        return $this->column;
    }
}
