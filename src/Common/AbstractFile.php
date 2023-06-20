<?php

namespace Tinderbox\Clickhouse\Common;

abstract class AbstractFile
{
    /**
     * Source file.
     */
    protected string $source;

    /**
     * File constructor.
     */
    public function __construct(string $source)
    {
        $this->source = $source;
    }

    /**
     * Returns full path to source file.
     */
    public function getSource(): string
    {
        return $this->source;
    }
}
