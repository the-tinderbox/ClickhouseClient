<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Common\File;
use Tinderbox\Clickhouse\Common\Format;
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
     * Http transport which provides http requests to server.
     *
     * @var TransportInterface
     */
    protected $transport;

    /**
     * Server provider.
     *
     * @var ServerProvider
     */
    protected $serverProvider;

    /**
     * Cluster name.
     *
     * @var string
     */
    protected $clusterName;

    /**
     * Server hostname.
     *
     * @var string
     */
    protected $serverHostname;

    /**
     * Client constructor.
     *
     * @param \Tinderbox\Clickhouse\ServerProvider                     $serverProvider
     * @param \Tinderbox\Clickhouse\Interfaces\TransportInterface|null $transport
     */
    public function __construct(
        ServerProvider $serverProvider,
        TransportInterface $transport = null
    ) {
        $this->serverProvider = $serverProvider;
        $this->setTransport($transport);
    }

    /**
     * Creates default http transport.
     *
     * @return HttpTransport
     */
    protected function createTransport()
    {
        return new HttpTransport();
    }

    /**
     * Sets transport.
     *
     * @param \Tinderbox\Clickhouse\Interfaces\TransportInterface|null $transport
     */
    protected function setTransport(TransportInterface $transport = null)
    {
        if (is_null($transport)) {
            $this->transport = $this->createTransport();
        } else {
            $this->transport = $transport;
        }
    }

    /**
     * Returns transport.
     *
     * @return TransportInterface
     */
    protected function getTransport(): TransportInterface
    {
        return $this->transport;
    }

    /**
     * Client will use servers from specified cluster.
     *
     * @param string|null $cluster
     *
     * @return $this
     */
    public function onCluster(?string $cluster)
    {
        $this->clusterName = $cluster;
        $this->serverHostname = null;

        return $this;
    }

    /**
     * Returns current cluster name.
     *
     * @return null|string
     */
    protected function getClusterName(): ?string
    {
        return $this->clusterName;
    }

    /**
     * Client will use specified server.
     *
     * @param string $serverHostname
     *
     * @return $this
     */
    public function using(string $serverHostname)
    {
        $this->serverHostname = $serverHostname;

        return $this;
    }

    /**
     * Client will return random server on each query.
     *
     * @return $this
     */
    public function usingRandomServer()
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
     *
     * @var string
     *
     * @return $this
     */
    public function usingServerWithTag(string $tag)
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
     * @return Server
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
     * @param string          $query
     * @param FileInterface[] $files
     * @param array           $settings
     *
     * @return \Tinderbox\Clickhouse\Query\Result
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
     * @param array $queries
     * @param int   $concurrency Max concurrency requests
     *
     * @return array
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
     * @param string $query
     * @param array  $files
     * @param array  $settings
     *
     * @return bool
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
     *
     * @param array $queries
     * @param int   $concurrency
     *
     * @return array
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
     * @param string      $table
     * @param array       $columns
     * @param array       $files
     * @param string|null $format
     * @param array       $settings
     * @param int         $concurrency Max concurrency requests
     *
     * @return array
     */
    public function writeFiles(
        string $table,
        array $columns,
        array $files,
        string $format = Format::TSV,
        array $settings = [],
        int $concurrency = 5
    ) {
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
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $sql
     * @param array                        $files
     * @param array                        $settings
     *
     * @return \Tinderbox\Clickhouse\Query
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
     * @param array $query
     *
     * @return \Tinderbox\Clickhouse\Query
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
