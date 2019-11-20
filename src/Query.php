<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Server;

/**
 * Query instance
 */
class Query
{
    /**
     * SQL Query.
     *
     * @var string
     */
    protected $query;

    /**
     * Files attached to query.
     *
     * @var array
     */
    protected $files = [];

    /**
     * Server to process query.
     *
     * @var \Tinderbox\Clickhouse\Server
     */
    protected $server;

    /**
     * Query settings
     *
     * @var array
     */
    protected $settings = [];

    /**
     * Response format
     *
     * @var string
     */
    protected $format = Format::JSON;

    /**
     * Query constructor.
     *
     * @param Server $server
     * @param string $query
     * @param array  $files
     * @param array  $settings
     * @param string $format
     */
    public function __construct(Server $server, string $query, array $files = [], array $settings = [], string $format = Format::JSON)
    {
        $this->server = $server;
        $this->query = $query;
        $this->files = $files;
        $this->settings = $settings;
        $this->format = $format;
    }

    /**
     * Returns SQL query.
     *
     * @return string
     */
    public function getQuery(): string
    {
        return $this->query;
    }

    /**
     * Returns files attached to query.
     *
     * @return array
     */
    public function getFiles(): array
    {
        return $this->files;
    }

    /**
     * Returns server to process query.
     *
     * @return \Tinderbox\Clickhouse\Server
     */
    public function getServer(): Server
    {
        return $this->server;
    }

    /**
     * Returns settings.
     *
     * @return array
     */
    public function getSettings(): array
    {
        return $this->settings;
    }

    /**
     * Returns format
     *
     * @return string
     */
    public function getFormat(): string
    {
        return $this->format;
    }
}
