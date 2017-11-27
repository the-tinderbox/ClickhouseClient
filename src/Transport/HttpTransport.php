<?php

namespace Tinderbox\Clickhouse\Transport;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\MultipartStream;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\ResponseInterface;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\ClientException;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Server;

/**
 * Http transport to perform queries.
 */
class HttpTransport implements TransportInterface
{
    /**
     * GuzzleClient.
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
     * Returns default headers for requests.
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
     * Sets Guzzle client.
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
     * Creates Guzzle client.
     */
    protected function createHttpClient()
    {
        return new Client();
    }

    /**
     * Sends query to given $server.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return bool
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
        } catch (ConnectException $e) {
            throw ClientException::connectionError();
        } catch (RequestException $e) {
            throw ClientException::serverReturnedError($e->getResponse()->getBody()->getContents().'. Query: '.$query);
        }
    }

    /**
     * Creates file handle to send file to server.
     *
     * @param string $file
     * @param bool   $gzip
     *
     * @return bool|resource
     */
    protected function getFileHandle(string $file, $gzip = true)
    {
        $fileHandle = fopen($file, 'r');

        if ($gzip) {
            stream_filter_append($fileHandle, 'zlib.deflate', STREAM_FILTER_READ, ['window' => 30]);
        }

        return $fileHandle;
    }

    /**
     * Sends async insert queries with given files.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param array                        $files
     * @param int                          $concurrency
     *
     * @return array
     */
    public function sendAsyncFilesWithQuery(Server $server, string $query, array $files, int $concurrency = 5): array
    {
        $requests = function ($files) use ($server, $query) {
            foreach ($files as $file) {
                $headers = array_merge($this->getHeaders(), [
                    'Content-Length' => null,
                ]);


                $request = new Request(
                    'POST', $this->buildRequestUri($server, ['query' => $query]), $headers, $this->getFileHandle($file)
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
                'rejected'    => $this->parseReason(),
                'options'     => [
                    'connect_timeout' => $server->getOptions()->getTimeout(),
                    'expect'          => false,
                ],
            ]
        );

        $promise = $pool->promise();

        $promise->wait();

        ksort($result);

        return $result;
    }

    /**
     * Assembles string from TempTable structure.
     *
     * @param \Tinderbox\Clickhouse\Common\TempTable $table
     *
     * @return string
     */
    protected function assembleTempTableStructure(TempTable $table)
    {
        $structure = $table->getStructure();
        $withColumns = true;

        $preparedStructure = [];

        foreach ($structure as $column => $type) {
            if (is_int($column)) {
                $withColumns = false;
                $preparedStructure[] = $type;
            } else {
                $preparedStructure[] = $column.' '.$type;
            }
        }

        return [implode(', ', $preparedStructure), $withColumns];
    }

    /**
     * Parse temp table data to append it to request.
     *
     * @param \Tinderbox\Clickhouse\Common\TempTable $table
     *
     * @return array
     */
    protected function parseTempTable(TempTable $table)
    {
        list($structure, $withColumns) = $this->assembleTempTableStructure($table);

        return [
            [
                $table->getName().'_'.($withColumns ? 'structure' : 'types') => $structure,
                $table->getName().'_format'                                  => $table->getFormat(),
            ],
            $this->getFileHandle($table->getSource(), false),
        ];
    }

    /**
     * Executes SELECT queries and returns result.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param TempTable|array|null         $tables
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return \Tinderbox\Clickhouse\Query\Result
     */
    public function get(Server $server, string $query, $tables = null): Result
    {
        try {
            $options = [
                'headers'         => $this->getHeaders(),
                'connect_timeout' => $server->getOptions()->getTimeout(),
            ];

            if ($tables instanceof TempTable || !empty($tables)) {
                $params = [
                    'query'             => $query,
                    'wait_end_of_query' => 1,
                ];

                if ($tables instanceof TempTable) {
                    $tables = [$tables];
                }

                $options['multipart'] = [];

                foreach ($tables as $table) {
                    list($tableQuery, $tableSourceHandle) = $this->parseTempTable($table);

                    $options['multipart'][] = [
                        'name'     => $table->getName(),
                        'contents' => $tableSourceHandle,
                    ];

                    $params = array_merge($tableQuery, $params);
                }
            } else {
                $params = [];
                $options['body'] = gzencode($query);
            }

            $response = $this->httpClient->post(
                $this->buildRequestUri($server, $params),
                $options
            );

            return $this->assembleResult($response);
        } catch (ConnectException $e) {
            throw ClientException::connectionError();
        } catch (RequestException $e) {
            throw ClientException::serverReturnedError($e->getResponse()->getBody()->getContents().'. Query: '.$query);
        }
    }

    /**
     * Determines the reason why request was rejected.
     *
     * @return \Closure
     */
    protected function parseReason()
    {
        return function ($reason, $index) {
            if ($reason instanceof RequestException) {
                $response = $reason->getResponse();

                if (is_null($response)) {
                    throw ClientException::connectionError();
                } else {
                    throw ClientException::serverReturnedError($reason->getResponse()->getBody()->getContents());
                }
            }

            throw $reason;
        };
    }

    /**
     * Executes async SELECT queries and returns result.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param array                        $queries
     * @param int                          $concurrency
     *
     * @return array
     */
    public function getAsync(Server $server, array $queries, int $concurrency = 5): array
    {
        $requests = function ($queries) use ($server) {
            foreach ($queries as $query) {
                $tables = $query[1] ?? null;
                $query = $query[0];

                if ($tables instanceof TempTable || !empty($tables)) {
                    $params = [
                        'query'             => $query,
                        'wait_end_of_query' => 1,
                    ];

                    if ($tables instanceof TempTable) {
                        $tables = [$tables];
                    }

                    $multipart = [];

                    foreach ($tables as $table) {
                        list($tableQuery, $tableSourceHandle) = $this->parseTempTable($table);

                        $multipart[] = [
                            'name'     => $table->getName(),
                            'contents' => $tableSourceHandle,
                        ];

                        $params = array_merge($tableQuery, $params);
                    }

                    $body = new MultipartStream($multipart);
                } else {
                    $params = [];
                    $body = gzencode($query);
                }

                yield new Request('POST', $this->buildRequestUri($server, $params), $this->getHeaders(), $body);
            }
        };

        $result = [];

        $pool = new Pool(
            $this->httpClient, $requests($queries), [
                'concurrency' => $concurrency,
                'fulfilled'   => function ($response, $index) use (&$result) {
                    $result[$index] = $this->assembleResult($response);
                },
                'rejected'    => $this->parseReason(),
                'options'     => [
                    'connect_timeout' => $server->getOptions()->getTimeout(),
                    'expect'          => false,
                ],
            ]
        );

        $promise = $pool->promise();

        $promise->wait();

        ksort($result);

        return $result;
    }

    /**
     * Assembles Result instance from server response.
     *
     * @param \Psr\Http\Message\ResponseInterface $response
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return \Tinderbox\Clickhouse\Query\Result
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
     * Builds uri with necessary params.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param array                        $query
     *
     * @return string
     */
    protected function buildRequestUri(Server $server, array $query = []): string
    {
        $uri = $server->getOptions()->getProtocol().'://'.$server->getHost().':'.$server->getPort();

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
