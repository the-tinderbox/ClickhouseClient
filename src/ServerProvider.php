<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Exceptions\ServerProviderException;

/**
 * Provides server from cluster, or just standalone server to perform queries
 */
class ServerProvider
{
    /**
     * Clusters
     *
     * @var Cluster[]
     */
    protected $clusters = [];

    /**
     * Servers
     *
     * @var Server[]
     */
    protected $servers = [];

    /**
     * Current server to perform queries
     *
     * @var Server
     */
    protected $currentServer;

    /**
     * Current cluster to select server
     *
     * @var Cluster
     */
    protected $currentCluster;

    public function addCluster(Cluster $cluster)
    {
        $clusterName = $cluster->getName();

        if (isset($this->clusters[$clusterName])) {
            throw ServerProviderException::clusterExists($clusterName);
        }

        $this->clusters[$clusterName] = $cluster;

        return $this;
    }

    public function addServer(Server $server)
    {
        $serverHostname = $server->getHost();

        if (isset($this->servers[$serverHostname])) {
            throw ServerProviderException::serverHostnameDuplicate($serverHostname);
        }

        $this->servers[$serverHostname] = $server;

        return $this;
    }

    public function getRandomServer(): Server
    {
        return $this->getServer(array_rand($this->servers, 1));
    }

    public function getRandomServerFromCluster(string $cluster): Server
    {
        $cluster = $this->getCluster($cluster);
        $randomServerIndex = array_rand($cluster->getServers(), 1);

        return $cluster->getServerByHostname($randomServerIndex);
    }

    public function getServerFromCluster(string $cluster, string $serverHostname)
    {
        $cluster = $this->getCluster($cluster);

        return $cluster->getServerByHostname($serverHostname);
    }

    public function getServer(string $serverHostname): Server
    {
        if (!isset($this->servers[$serverHostname])) {
            throw ServerProviderException::serverHostnameNotFound($serverHostname);
        }

        return $this->servers[$serverHostname];
    }

    public function getCluster(string $cluster) : Cluster
    {
        if (!isset($this->clusters[$cluster])) {
            throw ServerProviderException::clusterNotFound($cluster);
        }

        return $this->clusters[$cluster];
    }
}