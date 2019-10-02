<?php

namespace Tinderbox\Clickhouse;

/**
 * Query instance.
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
     * Query settings.
     *
     * @var array
     */
    protected $settings = [];

    /**
     * Query constructor.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param array                        $files
     * @param array                        $settings
     */
    public function __construct(Server $server, string $query, array $files = [], array $settings = [])
    {
        $this->server = $server;
        $this->query = $query;
        $this->files = $files;
        $this->settings = $settings;
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
}
