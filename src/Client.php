<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\ClientException;
use Tinderbox\Clickhouse\Interfaces\QueryMapperInterface;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query\Mapper\UnnamedMapper;
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
     * Values Mapper to raw  sql query.
     *
     * @var \Tinderbox\Clickhouse\Interfaces\QueryMapperInterface
     */
    protected $mapper;

    /**
     * Server to perform requests.
     *
     * @var \Tinderbox\Clickhouse\Server
     */
    protected $server;

    /**
     * Cluster to perform requests.
     *
     * In case of using cluster server will be chosen automatically or you may specify server by call using method
     *
     * @var \Tinderbox\Clickhouse\Cluster
     */
    protected $cluster;

    /**
     * Tells client to send queries over whole cluster selecting server by random
     *
     * @var bool
     */
    protected $useRandomServer = false;

    /**
     * Client constructor.
     *
     * @param \Tinderbox\Clickhouse\Server|\Tinderbox\Clickhouse\Cluster $server
     * @param \Tinderbox\Clickhouse\Interfaces\QueryMapperInterface|null $mapper
     * @param \Tinderbox\Clickhouse\Interfaces\TransportInterface|null   $transport
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     */
    public function __construct($server, QueryMapperInterface $mapper = null, TransportInterface $transport = null)
    {
        $serverType = get_class($server);

        switch ($serverType) {
            case Server::class:
                $this->setServer($server);
                break;

            case Cluster::class:
                $this->setCluster($server);
                break;

            default:
                throw ClientException::invalidServerProvided($server);
                break;
        }

        $this->setTransport($transport);
        $this->setMapper($mapper);
    }

    /**
     * Sets flag to use random server in cluster
     *
     * @param bool $flag
     */
    public function useRandomServer(bool $flag)
    {
        $this->useRandomServer = $flag;
    }

    /**
     * Returns flag which tells client to use random server in cluster
     *
     * @return bool
     */
    protected function shouldUseRandomServer() : bool
    {
        return !is_null($this->getCluster()) && $this->useRandomServer;
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
     * Returns values Mapper.
     *
     * @return \Tinderbox\Clickhouse\Interfaces\QueryMapperInterface
     */
    public function getMapper(): QueryMapperInterface
    {
        return $this->mapper;
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
     * Sets Mapper.
     *
     * @param \Tinderbox\Clickhouse\Interfaces\QueryMapperInterface $mapper
     *
     * @return \Tinderbox\Clickhouse\Client
     */
    public function setMapper(QueryMapperInterface $mapper = null): self
    {
        if (is_null($mapper)) {
            return $this->setDefaultMapper();
        }

        $this->mapper = $mapper;

        return $this;
    }

    /**
     * Sets default mapper.
     *
     * @return Client
     */
    protected function setDefaultMapper(): self
    {
        $this->mapper = new UnnamedMapper();

        return $this;
    }

    /**
     * Sets server to perform requests.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     *
     * @return \Tinderbox\Clickhouse\Client
     */
    public function setServer(Server $server): self
    {
        $this->server = $server;

        return $this;
    }

    /**
     * Returns current server.
     *
     * @return \Tinderbox\Clickhouse\Server
     */
    public function getServer(): Server
    {
        if ($this->shouldUseRandomServer()) {
            return $this->getRandomServer();
        }

        return $this->server;
    }

    /**
     * Returns random server from cluster
     *
     * @return \Tinderbox\Clickhouse\Server
     */
    public function getRandomServer() : Server
    {
        $cluster = $this->getCluster();
        $servers = $cluster->getServers();
        $random = array_rand($servers, 1);

        return $servers[$random];
    }

    /**
     * Sets cluster.
     *
     * If you have previously used alone server and then want to use cluster, server will be chosen from cluster
     *
     * @param \Tinderbox\Clickhouse\Cluster $cluster
     * @param string|null                   $defaultServerHostname Default server to perform requests
     *
     * @return \Tinderbox\Clickhouse\Client
     */
    public function setCluster(Cluster $cluster, string $defaultServerHostname = null): self
    {
        $this->server = null;
        $this->cluster = $cluster;

        $this->setServerByDefaultHostname($defaultServerHostname);

        return $this;
    }

    /**
     * Gets server from cluster and uses him to perform requests.
     *
     * If no hostname provided, will use first server in cluster
     *
     * @param string|null $hostname
     */
    protected function setServerByDefaultHostname(string $hostname = null)
    {
        if (is_null($hostname)) {
            $servers = $this->getCluster()->getServers();
            $hostnames = array_keys($servers);

            $hostname = $hostnames[0];
        }

        $this->using($hostname);
    }

    /**
     * Switches between servers in cluster.
     *
     * If no cluster provided throws exception
     *
     * @param string $hostname
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return \Tinderbox\Clickhouse\Client
     */
    public function using(string $hostname): self
    {
        if (is_null($this->getCluster())) {
            throw ClientException::clusterIsNotProvided();
        }

        $this->setServer($this->getCluster()->getServerByHostname($hostname));

        return $this;
    }

    /**
     * Return cluster.
     *
     * @return null|\Tinderbox\Clickhouse\Cluster
     */
    public function getCluster(): ?Cluster
    {
        return $this->cluster;
    }

    /**
     * Removes cluster to use alone server.
     *
     * @return \Tinderbox\Clickhouse\Client
     */
    public function removeCluster(): self
    {
        $this->cluster = null;

        return $this;
    }

    /**
     * Returns transport.
     *
     * @return TransportInterface
     */
    protected function getTransport() : TransportInterface
    {
        return $this->transport;
    }

    /**
     * Performs select query.
     *
     * Example:
     *
     * $client->select('select * from table where column = ?', [1]);
     *
     * @param string               $query
     * @param array                $bindings
     * @param TempTable|array|null $tables
     *
     * @return \Tinderbox\Clickhouse\Query\Result
     */
    public function select(string $query, array $bindings = [], $tables = null): Result
    {
        $query = $this->prepareQuery($query, $bindings).' FORMAT JSON';

        return $this->getTransport()->get($this->getServer(), $query, $tables);
    }

    /**
     * Performs async select queries.
     *
     * Example:
     *
     * $client->selectAsync([
     *      ['select * from table where column = ?', [1]],
     *      ['select * from table where column = ?', [2]],
     *      ['select * from table where column = ?', [3]],
     * ]);
     *
     * @param array $queries
     * @param int   $concurrency Max concurrency requests
     *
     * @return array
     */
    public function selectAsync(array $queries, int $concurrency = 5): array
    {
        foreach ($queries as $i => $query) {
            $queries[$i] = [$this->prepareQuery($query[0], $query[1] ?? []).' FORMAT JSON', $query[2] ?? null];
        }

        return $this->getTransport()->getAsync($this->getServer(), $queries, $concurrency);
    }

    /**
     * Performs insert query.
     *
     * Example:
     *
     * $client->insert('insert into table (column, column) values (?, ?), (?,?)', ['1', '2', '3', '4']);
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return bool
     */
    public function insert(string $query, array $bindings = []): bool
    {
        $query = $this->prepareQuery($query, $bindings);

        return $this->getTransport()->send($this->getServer(), $query);
    }

    /**
     * Performs async insert queries using local csv or tsv files.
     *
     * Example:
     *
     * $result = $client->insertFiles('table', ['column', 'column'], [
     *      'file1.csv',
     *      'file2.csv',
     *      'file3.csv',
     *      'file4.csv',
     * ]);
     *
     * @param string      $table
     * @param array       $columns
     * @param array       $files
     * @param string|null $format
     * @param int         $concurrency Max concurrency requests
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return array
     */
    public function insertFiles(string $table, array $columns, array $files, string $format = null, int $concurrency = 5)
    {
        if (is_null($format)) {
            $format = Format::CSV;
        }

        $query = 'INSERT INTO '.$table.' ('.implode(', ', $columns).') FORMAT '.strtoupper($format);

        foreach ($files as $file) {
            if (!is_file($file)) {
                throw ClientException::insertFileNotFound($file);
            }
        }

        return $this->getTransport()->sendAsyncFilesWithQuery($this->getServer(), $query, $files, $concurrency);
    }

    /**
     * Executes query.
     *
     * Alias for method insert
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return bool
     */
    public function statement(string $query, array $bindings = []): bool
    {
        return $this->insert($query, $bindings);
    }

    /**
     * Prepares query to execution.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return string
     */
    protected function prepareQuery(string $query, array $bindings)
    {
        return $this->getMapper()->bind($query, $bindings);
    }
}
