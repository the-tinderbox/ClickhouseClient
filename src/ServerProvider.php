<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Exceptions\ServerProviderException;

/**
 * Provides server from cluster, or just standalone server to perform queries.
 */
class ServerProvider
{
    /**
     * Clusters.
     *
     * @var Cluster[]
     */
    protected $clusters = [];

    /**
     * Servers.
     *
     * @var Server[]
     */
    protected $servers = [];

    /**
     * Servers by tags.
     *
     * @var Server[][]
     */
    protected $serversByTags = [];

    /**
     * Current server to perform queries.
     *
     * @var Server
     */
    protected $currentServer;

    /**
     * Current cluster to select server.
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

        $serverTags = $server->getOptions()->getTags();

        foreach ($serverTags as $serverTag) {
            $this->serversByTags[$serverTag][$serverHostname] = true;
        }

        return $this;
    }

    public function getRandomServer(): Server
    {
        return $this->getServer(array_rand($this->servers, 1));
    }

    public function getRandomServerWithTag(string $tag): Server
    {
        if (!isset($this->serversByTags[$tag])) {
            throw ServerProviderException::serverTagNotFound($tag);
        }

        return $this->getServer(array_rand($this->serversByTags[$tag], 1));
    }

    public function getRandomServerFromCluster(string $cluster): Server
    {
        $cluster = $this->getCluster($cluster);
        $randomServerHostname = array_rand($cluster->getServers(), 1);

        return $cluster->getServerByHostname($randomServerHostname);
    }

    public function getRandomServerFromClusterByTag(string $cluster, string $tag): Server
    {
        $cluster = $this->getCluster($cluster);

        $randomServerHostname = array_rand($cluster->getServersByTag($tag), 1);

        return $cluster->getServerByHostname($randomServerHostname);
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

    public function getCluster(string $cluster): Cluster
    {
        if (!isset($this->clusters[$cluster])) {
            throw ServerProviderException::clusterNotFound($cluster);
        }

        return $this->clusters[$cluster];
    }
}
