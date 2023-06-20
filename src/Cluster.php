<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Exceptions\ClusterException;

/**
 * Cluster - is a container with many Server instances.
 */
class Cluster
{
    /**
     * Servers in cluster.
     *
     * @var Server[]
     */
    protected array $servers = [];

    /**
     * Servers in cluster by tags.
     *
     * @var Server[][]
     */
    protected array $serversByTags = [];

    /**
     * Cluster constructor.
     *
     * @throws ClusterException
     */
    public function __construct(
        /**
         * Cluster name like in configuration file.
         */
        protected string $name,
        array $servers
    )
    {
        $this->addServers($servers);
    }

    /**
     * Pushes servers to cluster.
     *
     * @param array $servers Each server can be provided as array or Server instance
     *
     * @throws ClusterException
     */
    public function addServers(array $servers): self
    {
        foreach ($servers as $hostname => $server) {
            if (!$server instanceof Server && is_array($server)) {
                $host = $server['host'];
                $port = $server['port'] ?? null;
                $database = $server['database'] ?? null;
                $username = $server['username'] ?? null;
                $password = $server['password'] ?? null;
                $options = $server['options'] ?? null;

                $server = new Server($host, $port, $database, $username, $password, $options);
            }

            /* @var Server $server */
            if (!is_string($hostname)) {
                $hostname = $server->getHost();
            }

            $this->addServer($hostname, $server);
        }

        return $this;
    }

    /**
     * Pushes one server to cluster.
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClusterException
     */
    public function addServer(string $hostname, Server $server): void
    {
        if (isset($this->servers[$hostname])) {
            throw ClusterException::serverHostnameDuplicate($hostname);
        }

        $this->servers[$hostname] = $server;

        $serverTags = $server->getOptions()->getTags();

        foreach ($serverTags as $serverTag) {
            $this->serversByTags[$serverTag][$hostname] = true;
        }
    }

    /**
     * Returns servers in cluster.
     *
     * @return Server[]
     */
    public function getServers(): array
    {
        return $this->servers;
    }

    /**
     * Returns servers in cluster by tag.
     *
     * @throws ClusterException
     *
     * @return Server[]
     */
    public function getServersByTag(string $tag): array
    {
        if (!isset($this->serversByTags[$tag])) {
            throw ClusterException::tagNotFound($tag);
        }

        return $this->serversByTags[$tag];
    }

    /**
     * Returns server by specified hostname.
     *
     * @throws ClusterException
     */
    public function getServerByHostname(string $hostname): Server
    {
        if (!isset($this->servers[$hostname])) {
            throw ClusterException::serverNotFound($hostname);
        }

        return $this->servers[$hostname];
    }

    /**
     * Returns cluster name.
     */
    public function getName(): string
    {
        return $this->name;
    }
}
