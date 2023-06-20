<?php

namespace Tinderbox\Clickhouse;

/**
 * Query instance.
 */
class Query
{
    /**
     * Query constructor.
     */
    public function __construct(
        /**
         * Server to process query.
         */
        protected Server $server,
        /**
         * SQL Query.
         */
        protected string $query,
        /**
         * Files attached to query.
         */
        protected array $files = [],
        /**
         * Query settings.
         */
        protected array $settings = []
    ) {}

    /**
     * Returns SQL query.
     */
    public function getQuery(): string
    {
        return $this->query;
    }

    /**
     * Returns files attached to query.
     */
    public function getFiles(): array
    {
        return $this->files;
    }

    /**
     * Returns server to process query.
     */
    public function getServer(): Server
    {
        return $this->server;
    }

    /**
     * Returns settings.
     */
    public function getSettings(): array
    {
        return $this->settings;
    }
}
