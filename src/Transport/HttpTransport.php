<?php

namespace Tinderbox\Clickhouse\Transport;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\ResponseInterface;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\Exceptions\ClientException;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Query\QueryStatistic;

/**
 * Http transport to perform queries
 */
class HttpTransport implements TransportInterface
{
    /**
     * GuzzleClient
     *
     * @var Client
     */
    protected $httpClient;
    
    /**
     * HttpTransport constructor.
     *
     * @param Client $client
     */
    public function __construct(Client $client = null)
    {
        $this->setClient($client);
    }
    
    /**
     * Returns default headers for requests
     *
     * @return array
     */
    protected function getHeaders()
    {
        return [
            'Accept-Encoding'  => 'gzip',
            'Content-Encoding' => 'gzip',
        ];
    }
    
    /**
     * Sets Guzzle client
     *
     * @param Client|null $client
     */
    protected function setClient(Client $client = null)
    {
        if (is_null($client)) {
            $this->httpClient = $this->createHttpClient();
        } else {
            $this->httpClient = $client;
        }
    }
    
    /**
     * Creates Guzzle client
     */
    protected function createHttpClient()
    {
        return new Client();
    }
    
    /**
     * Sends query to given $server
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                             $query
     *
     * @return bool
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     */
    public function send(Server $server, string $query): bool
    {
        try {
            $this->httpClient->post(
                $this->buildRequestUri($server),
                [
                    'headers'         => $this->getHeaders(),
                    'body'            => gzencode($query),
                    'connect_timeout' => $server->getOptions()->getTimeout(),
                ]
            );
            
            return true;
        } catch (RequestException $e) {
            throw ClientException::serverReturnedError($e->getResponse()->getBody()->getContents().'. Query: '.$query);
        }
    }
    
    /**
     * Sends async insert queries with given files
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                             $query
     * @param array                              $files
     * @param int                                $concurrency
     *
     * @return array
     */
    public function sendAsyncFilesWithQuery(Server $server, string $query, array $files, int $concurrency = 5): array
    {
        $requests = function ($files) use ($server, $query) {
            foreach ($files as $file) {
                $fileHandle = fopen($file, 'r');
                
                stream_filter_append($fileHandle, 'zlib.deflate', STREAM_FILTER_READ, ['window' => 30]);
                
                $request = new Request(
                    'POST', $this->buildRequestUri($server, ['query' => $query,]), $this->getHeaders(), $fileHandle
                );
                
                yield $request;
            }
        };
        
        $result = [];
        
        $pool = new Pool(
            $this->httpClient, $requests($files), [
                'concurrency' => $concurrency,
                'fulfilled'   => function ($response, $index) use (&$result) {
                    $result[$index] = true;
                },
                'rejected'    => function ($reason, $index) {
                    throw ClientException::serverReturnedError($reason->getResponse()->getBody()->getContents());
                },
            ]
        );
        
        $promise = $pool->promise();
        
        $promise->wait();
        
        ksort($result);
        
        return $result;
    }
    
    /**
     * Executes SELECT queries and returns result
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                             $query
     *
     * @return \Tinderbox\Clickhouse\Query\Result
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     */
    public function get(Server $server, string $query): Result
    {
        try {
            $response = $this->httpClient->post(
                $this->buildRequestUri($server),
                [
                    'headers'         => $this->getHeaders(),
                    'body'            => gzencode($query),
                    'connect_timeout' => $server->getOptions()->getTimeout(),
                ]
            );
            
            return $this->assembleResult($response);
        } catch (RequestException $e) {
            throw ClientException::serverReturnedError($e->getResponse()->getBody()->getContents().'. Query: '.$query);
        }
    }
    
    /**
     * Executes async SELECT queries and returns result
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param array                              $queries
     * @param int                                $concurrency
     *
     * @return array
     */
    public function getAsync(Server $server, array $queries, int $concurrency = 5): array
    {
        $requests = function ($queries) use ($server) {
            foreach ($queries as $query) {
                yield new Request('POST', $this->buildRequestUri($server), $this->getHeaders(), $query);
            }
        };
        
        $result = [];
        
        $pool = new Pool(
            $this->httpClient, $requests($queries), [
                'concurrency' => $concurrency,
                'fulfilled'   => function ($response, $index) use (&$result) {
                    $result[$index] = $this->assembleResult($response);
                },
                'rejected'    => function ($reason, $index) {
                    throw ClientException::serverReturnedError($reason->getResponse()->getBody()->getContents());
                },
            ]
        );
        
        $promise = $pool->promise();
        
        $promise->wait();
        
        ksort($result);
        
        return $result;
    }
    
    /**
     * Assembles Result instance from server response
     *
     * @param \Psr\Http\Message\ResponseInterface $response
     *
     * @return \Tinderbox\Clickhouse\Query\Result
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     */
    protected function assembleResult(ResponseInterface $response): Result
    {
        $response = $response->getBody()->getContents();
        
        try {
            $result = \GuzzleHttp\json_decode($response, true);
    
            $statistic = new QueryStatistic(
                $result['statistics']['rows_read'] ?? 0,
                $result['statistics']['bytes_read'] ?? 0,
                $result['statistics']['elapsed'] ?? 0
            );
            
            return new Result($result['data'] ?? [], $statistic);
        } catch (\Exception $e) {
            throw ClientException::malformedResponseFromServer($response);
        }
    }
    
    /**
     * Builds uri with necessary params
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param array                              $query
     *
     * @return string
     */
    protected function buildRequestUri(Server $server, array $query = []): string
    {
        $uri = $server->getOptions()->getProtocol().'://'.$server->getHost().':'.$server->getPort();
        
        $query = array_merge($query, [
            'wait_end_of_query' => 1,
        ]);
        
        if (!is_null($server->getDatabase())) {
            $query['database'] = $server->getDatabase();
        }
        
        if (!is_null($server->getUsername())) {
            $query['user'] = $server->getUsername();
        }
        
        if (!is_null($server->getPassword())) {
            $query['password'] = $server->getPassword();
        }
        
        return $uri.'?'.http_build_query($query);
    }
}