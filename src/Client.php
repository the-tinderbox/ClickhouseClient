<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Common\File;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Exceptions\ClusterException;
use Tinderbox\Clickhouse\Exceptions\ServerProviderException;
use Tinderbox\Clickhouse\Interfaces\FileInterface;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Transport\HttpTransport;

/**
 * Client.
 */
class Client
{
    /**
     * Cluster name.
     */
    protected ?string $clusterName = null;

    /**
     *  Http transport which provides http requests to server.
     */
    protected ?TransportInterface $transport = null;

    /**
     * Server hostname.
     */
    protected null|string|\Closure $serverHostname = null;

    /**
     * Client constructor.
     */
    public function __construct(
        /**
         * Server Provider.
         */
        protected ServerProvider $serverProvider,
        ?TransportInterface $transport = null
    ) {
        $this->setTransport($transport);
    }

    /**
     * Creates default http transport.
     */
    protected function createTransport(): HttpTransport
    {
        return new HttpTransport();
    }

    /**
     * Sets transport.
     */
    protected function setTransport(?TransportInterface $transport = null): void
    {
        if (is_null($transport)) {
            $this->transport = $this->createTransport();
        } else {
            $this->transport = $transport;
        }
    }

    /**
     * Returns transport.
     */
    protected function getTransport(): TransportInterface
    {
        return $this->transport;
    }

    /**
     * Client will use servers from specified cluster.
     */
    public function onCluster(?string $cluster): self
    {
        $this->clusterName = $cluster;
        $this->serverHostname = null;

        return $this;
    }

    /**
     * Returns current cluster name.
     */
    protected function getClusterName(): ?string
    {
        return $this->clusterName;
    }

    /**
     * Client will use specified server.
     */
    public function using(string $serverHostname): self
    {
        $this->serverHostname = $serverHostname;

        return $this;
    }

    /**
     * Client will return random server on each query.
     */
    public function usingRandomServer(): self
    {
        $this->serverHostname = function () {
            if ($this->isOnCluster()) {
                return $this->serverProvider->getRandomServerFromCluster($this->getClusterName());
            } else {
                return $this->serverProvider->getRandomServer();
            }
        };

        return $this;
    }

    /**
     * Client will use server with tag as server for queries.
     */
    public function usingServerWithTag(string $tag): self
    {
        $this->serverHostname = function () use ($tag) {
            if ($this->isOnCluster()) {
                return $this->serverProvider->getRandomServerFromClusterByTag($this->getClusterName(), $tag);
            } else {
                return $this->serverProvider->getRandomServerWithTag($tag);
            }
        };

        return $this;
    }

    /**
     * Returns true if cluster selected.
     *
     * @return bool
     */
    protected function isOnCluster(): bool
    {
        return !is_null($this->getClusterName());
    }

    /**
     * Returns server to perform request.
     *
     * @throws ServerProviderException
     * @throws ClusterException
     */
    public function getServer(): Server
    {
        if ($this->serverHostname instanceof \Closure) {
            $server = call_user_func($this->serverHostname);
        } else {
            if ($this->isOnCluster()) {
                /*
                 * If no server provided, will take random server from cluster
                 */
                if (is_null($this->serverHostname)) {
                    $server = $this->serverProvider->getRandomServerFromCluster($this->getClusterName());
                    $this->serverHostname = $server->getHost();
                } else {
                    $server = $this->serverProvider->getServerFromCluster(
                        $this->getClusterName(),
                        $this->serverHostname
                    );
                }
            } else {
                /*
                 * If no server provided, will take random server from cluster
                 */
                if (is_null($this->serverHostname)) {
                    $server = $this->serverProvider->getRandomServer();
                    $this->serverHostname = $server->getHost();
                } else {
                    $server = $this->serverProvider->getServer($this->serverHostname);
                }
            }
        }

        return $server;
    }

    /**
     * Performs select query and returns one result.
     *
     * Example:
     *
     * $client->select('select * from table where column = ?', [1]);
     *
     * @param FileInterface[] $files
     * @throws ServerProviderException
     * @throws ClusterException
     */
    public function readOne(string $query, array $files = [], array $settings = []): Result
    {
        $query = $this->createQuery($this->getServer(), $query, $files, $settings);

        $result = $this->getTransport()->read([$query], 1);

        return $result[0];
    }

    /**
     * Performs batch of select queries.
     *
     * @param int $concurrency Max concurrency requests
     * @throws ServerProviderException
     */
    public function read(array $queries, int $concurrency = 5): array
    {
        foreach ($queries as $i => $query) {
            if (!$query instanceof Query) {
                $queries[$i] = $this->guessQuery($query);
            }
        }

        return $this->getTransport()->read($queries, $concurrency);
    }

    /**
     * Performs insert or simple statement query.
     *
     * @throws ServerProviderException
     * @throws ClusterException
     */
    public function writeOne(string $query, array $files = [], array $settings = []): bool
    {
        if (!$query instanceof Query) {
            $query = $this->createQuery($this->getServer(), $query, $files, $settings);
        }

        $result = $this->getTransport()->write([$query], 1);

        return $result[0][0];
    }

    /**
     * Performs batch of insert or simple statement queries.
     */
    public function write(array $queries, int $concurrency = 5): array
    {
        foreach ($queries as $i => $query) {
            if (!$query instanceof Query) {
                $queries[$i] = $this->guessQuery($query);
            }
        }

        return $this->getTransport()->write($queries, $concurrency);
    }

    /**
     * Performs async insert queries using local csv or tsv files.
     *
     * @param int $concurrency Max concurrency requests
     * @throws ServerProviderException
     * @throws ClusterException
     */
    public function writeFiles(
        string $table,
        array $columns,
        array $files,
        ?string $format = Format::TSV,
        array $settings = [],
        int $concurrency = 5
    ): array {
        $sql = 'INSERT INTO '.$table.' ('.implode(', ', $columns).') FORMAT '.strtoupper($format);

        foreach ($files as $i => $file) {
            if (!$file instanceof FileInterface) {
                $files[$i] = new File($file);
            }
        }

        $query = $this->createQuery($this->getServer(), $sql, $files, $settings);

        return $this->getTransport()->write([$query], $concurrency);
    }

    /**
     * Creates query instance from specified arguments.
     */
    protected function createQuery(
        Server $server,
        string $sql,
        array $files = [],
        array $settings = []
    ): Query {
        return new Query($server, $sql, $files, $settings);
    }

    /**
     * Parses query array and returns query instance.
     *
     * @throws ServerProviderException
     * @throws ClusterException
     */
    protected function guessQuery(array $query): Query
    {
        $server = $query['server'] ?? $this->getServer();
        $sql = $query['query'];
        $tables = $query['files'] ?? [];
        $settings = $query['settings'] ?? [];

        return $this->createQuery($server, $sql, $tables, $settings);
    }
}
