<?php

namespace Tinderbox\Clickhouse\Common;

/**
 * Temporary table for select requests which receives data from local file.
 */
class TempTable
{
    /**
     * Table name to use in query in where section.
     *
     * @var string
     */
    protected $name;

    /**
     * Table structure to map data in file on table.
     *
     * @var array
     */
    protected $structure = [];

    /**
     * Source file.
     *
     * @var string
     */
    protected $source;

    /**
     * Source data format.
     *
     * @var string
     */
    protected $format;

    /**
     * TempTable constructor.
     *
     * @param string $name
     * @param string $source
     * @param array  $structure
     * @param string $format
     */
    public function __construct(string $name, string $source, array $structure, string $format = Format::CSV)
    {
        $this->setName($name);
        $this->setSource($source);
        $this->setStructure($structure);
        $this->setFormat($format);
    }

    /**
     * Set name of table.
     *
     * @param string $name
     *
     * @return \Tinderbox\Clickhouse\Common\TempTable
     */
    public function setName(string $name): self
    {
        $this->name = $name;

        return $this;
    }

    /**
     * Set full path to source file.
     *
     * @param string $source
     *
     * @return \Tinderbox\Clickhouse\Common\TempTable
     */
    public function setSource(string $source): self
    {
        $this->source = $source;

        return $this;
    }

    /**
     * Set data format in source file.
     *
     * @param string $format
     *
     * @return \Tinderbox\Clickhouse\Common\TempTable
     */
    public function setFormat(string $format): self
    {
        $this->format = $format;

        return $this;
    }

    /**
     * Set table structure.
     *
     * @param array $structure
     *
     * @return \Tinderbox\Clickhouse\Common\TempTable
     */
    public function setStructure(array $structure): self
    {
        $this->structure = $structure;

        return $this;
    }

    /**
     * Returns table name.
     *
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
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

    /**
     * Returns format of data in source file.
     *
     * @return string
     */
    public function getFormat(): string
    {
        return $this->format;
    }

    /**
     * Returns table structure.
     *
     * @return array
     */
    public function getStructure(): array
    {
        return $this->structure;
    }
}
