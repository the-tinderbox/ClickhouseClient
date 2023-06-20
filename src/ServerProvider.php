<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Exceptions\ClusterException;
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
    protected array $clusters = [];

    /**
     * Servers.
     *
     * @var Server[]
     */
    protected array $servers = [];

    /**
     * Servers by tags.
     *
     * @var Server[][]
     */
    protected array $serversByTags = [];

    /**
     * Current server to perform queries.
     *
     * @var Server
     */
    protected Server $currentServer;

    /**
     * Current cluster to select server.
     *
     * @var Cluster
     */
    protected Cluster $currentCluster;

    /**
     * @throws ServerProviderException
     */
    public function addCluster(Cluster $cluster): self
    {
        $clusterName = $cluster->getName();

        if (isset($this->clusters[$clusterName])) {
            throw ServerProviderException::clusterExists($clusterName);
        }

        $this->clusters[$clusterName] = $cluster;

        return $this;
    }

    /**
     * @throws ServerProviderException
     */
    public function addServer(Server $server): self
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

    /**
     * @throws ServerProviderException
     */
    public function getRandomServer(): Server
    {
        return $this->getServer(array_rand($this->servers));
    }

    /**
     * @throws ServerProviderException
     */
    public function getRandomServerWithTag(string $tag): Server
    {
        if (!isset($this->serversByTags[$tag])) {
            throw ServerProviderException::serverTagNotFound($tag);
        }

        return $this->getServer(array_rand($this->serversByTags[$tag], 1));
    }

    /**
     * @throws ServerProviderException
     * @throws ClusterException
     */
    public function getRandomServerFromCluster(string $cluster): Server
    {
        $cluster = $this->getCluster($cluster);
        $randomServerHostname = array_rand($cluster->getServers());

        return $cluster->getServerByHostname($randomServerHostname);
    }

    /**
     * @throws ServerProviderException
     * @throws ClusterException
     */
    public function getRandomServerFromClusterByTag(string $cluster, string $tag): Server
    {
        $cluster = $this->getCluster($cluster);

        $randomServerHostname = array_rand($cluster->getServersByTag($tag));

        return $cluster->getServerByHostname($randomServerHostname);
    }

    /**
     * @throws ServerProviderException
     * @throws ClusterException
     */
    public function getServerFromCluster(string $cluster, string $serverHostname): Server
    {
        $cluster = $this->getCluster($cluster);

        return $cluster->getServerByHostname($serverHostname);
    }

    /**
     * @throws ServerProviderException
     */
    public function getServer(string $serverHostname): Server
    {
        if (!isset($this->servers[$serverHostname])) {
            throw ServerProviderException::serverHostnameNotFound($serverHostname);
        }

        return $this->servers[$serverHostname];
    }

    /**
     * @throws ServerProviderException
     */
    public function getCluster(string $cluster): Cluster
    {
        if (!isset($this->clusters[$cluster])) {
            throw ServerProviderException::clusterNotFound($cluster);
        }

        return $this->clusters[$cluster];
    }
}
