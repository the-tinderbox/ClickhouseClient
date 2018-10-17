<?php

namespace Tinderbox\Clickhouse;

class Query
{

    protected $query;

    protected $files = [];

    protected $server;

    protected $settings = [];

    public function __construct(Server $server, string $query, array $files = [], array $settings = [])
    {
        $this->server = $server;
        $this->query = $query;
        $this->files = $files;
        $this->settings = $settings;
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function getFiles(): array
    {
        return $this->files;
    }

    public function getServer(): Server
    {
        return $this->server;
    }

    public function getSettings(): array
    {
        return $this->settings;
    }
}