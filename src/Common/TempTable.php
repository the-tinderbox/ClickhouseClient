<?php

namespace Tinderbox\Clickhouse\Common;

use Psr\Http\Message\StreamInterface;
use Tinderbox\Clickhouse\Interfaces\FileInterface;

/**
 * Temporary table for select requests which receives data from local file.
 */
class TempTable implements FileInterface
{
    /**
     * Table name to use in query in where section.
     */
    protected string $name;

    /**
     * Table structure to map data in file on table.
     */
    protected array $structure = [];

    /**
     * Format.
     */
    protected string $format;

    /**
     * Source.
     */
    protected string|FileInterface $source;

    /**
     * TempTable constructor.
     */
    public function __construct(string $name, string|FileInterface $source, array $structure, string $format = Format::CSV)
    {
        $this->name = $name;
        $this->structure = $structure;
        $this->format = $format;

        $this->setSource($source);
    }

    protected function setSource($source): void
    {
        if (is_scalar($source)) {
            $source = new File($source);
        }

        $this->source = $source;
    }

    /**
     * Returns table name.
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Returns table structure.
     */
    public function getStructure(): array
    {
        return $this->structure;
    }

    /**
     * Returns format.
     */
    public function getFormat(): string
    {
        return $this->format;
    }

    public function open(bool $gzip = true): StreamInterface
    {
        return $this->source->open($gzip);
    }
}
