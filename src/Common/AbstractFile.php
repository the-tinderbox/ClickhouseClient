<?php

namespace Tinderbox\Clickhouse\Common;

abstract class AbstractFile
{
    /**
     * Source file.
     *
     * @var string
     */
    protected $source;

    /**
     * File constructor.
     *
     * @param string $source
     */
    public function __construct(string $source)
    {
        $this->source = $source;
    }

    /**
     * Returns full path to source file.
     *
     * @return string
     */
    public function getSource(): string
    {
        return $this->source;
    }
}
