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
     * @var \Tinderbox\Clickhouse\Server[]
     */
    protected $servers = [];

    /**
     * Cluster constructor.
     *
     * $servers may be empty, but when you decide to make request it will fail
     *
     * @param array $servers
     */
    public function __construct(array $servers = [])
    {
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
            if (!is_string($hostname)) {
                throw ClusterException::missingServerHostname();
            }

            if (!$server instanceof Server && is_array($server)) {
                $host = $server['host'];
                $port = $server['port'] ?? null;
                $database = $server['database'] ?? null;
                $username = $server['username'] ?? null;
                $password = $server['password'] ?? null;
                $options = $server['options'] ?? null;

                $server = new Server($host, $port, $database, $username, $password, $options);
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
}
