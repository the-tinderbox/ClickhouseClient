<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Exceptions\ClusterException;

/**
 * Cluster - is a container with many Server instances.
 */
class Cluster
{
    /**
     * Cluster name like in configuration file.
     *
     * @var string
     */
    protected $name;

    /**
     * Servers in cluster.
     *
     * @var \Tinderbox\Clickhouse\Server[]
     */
    protected $servers = [];

    /**
     * Servers in cluster by tags.
     *
     * @var \Tinderbox\Clickhouse\Server[][]
     */
    protected $serversByTags = [];

    /**
     * Cluster constructor.
     *
     * @param string $name
     * @param array  $servers
     *
     * @throws ClusterException
     */
    public function __construct(string $name, array $servers)
    {
        $this->name = $name;
        $this->addServers($servers);
    }

    /**
     * Pushes servers to cluster.
     *
     * @param array $servers Each server can be provided as array or Server instance
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClusterException
     *
     * @return \Tinderbox\Clickhouse\Cluster
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
     * @param string                       $hostname
     * @param \Tinderbox\Clickhouse\Server $server
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClusterException
     */
    public function addServer(string $hostname, Server $server)
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
     * @return \Tinderbox\Clickhouse\Server[]
     */
    public function getServers(): array
    {
        return $this->servers;
    }

    /**
     * Returns servers in cluster by tag.
     *
     * @param string $tag
     *
     * @throws ClusterException
     *
     * @return \Tinderbox\Clickhouse\Server[]
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
     * @param string $hostname
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClusterException
     *
     * @return \Tinderbox\Clickhouse\Server
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
     *
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }
}
