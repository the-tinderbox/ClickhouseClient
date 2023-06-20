<?php

namespace Tinderbox\Clickhouse\Transport;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\MultipartStream;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\ResponseInterface;
use Throwable;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\TransportException;
use Tinderbox\Clickhouse\Interfaces\FileInterface;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query;
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
     */
    protected Client $httpClient;

    /**
     * Array with three keys (read, write and deflate) with guzzle options for corresponding requests.
     *
     * [
     *   'read' => [
     *     'timeout' => 50,
     *     'connect_timeout => 10,
     *   ],
     *   'write' => [
     *     'debug' => true,
     *     'timeout' => 100,
     *   ],
     *   'deflate' => true
     * ]
     */
    private array $options;

    /**
     * HttpTransport constructor.
     */
    public function __construct(?Client $client = null, array $options = [])
    {
        $this->setClient($client);

        $this->options = $options;
    }

    /**
     * Returns flag to enable / disable queries and data compression.
     */
    protected function isDeflateEnabled(): bool
    {
        return $this->options['deflate'] ?? true;
    }

    /**
     * Returns default headers for requests.
     */
    protected function getHeaders(): array
    {
        $headers = [
            'Accept-Encoding'  => 'gzip',
        ];

        if ($this->isDeflateEnabled()) {
            $headers['Content-Encoding'] = 'gzip';
        }

        return $headers;
    }

    /**
     * Sets Guzzle client.
     */
    protected function setClient(?Client $client = null): void
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
    protected function createHttpClient(): Client
    {
        return new Client();
    }

    /**
     * Executes write queries.
     *
     * @throws Throwable
     */
    public function write(array $queries, int $concurrency = 5): array
    {
        $result = [];
        $openedStreams = [];

        foreach ($queries as $query) {
            $requests = function (Query $query) use (&$openedStreams) {
                if (!empty($query->getFiles())) {
                    foreach ($query->getFiles() as $file) {
                        /* @var FileInterface $file */
                        $headers = $this->getHeaders();

                        $uri = $this->buildRequestUri($query->getServer(), [
                            'query' => $query->getQuery(),
                        ], $query->getSettings());

                        $stream = $file->open($this->isDeflateEnabled());
                        $openedStreams[] = $stream;

                        $request = new Request('POST', $uri, $headers, $stream);

                        yield $request;
                    }
                } else {
                    $headers = $this->getHeaders();

                    $uri = $this->buildRequestUri($query->getServer(), [], $query->getSettings());

                    $sql = $this->isDeflateEnabled() ? gzencode($query->getQuery()) : $query->getQuery();
                    $request = new Request('POST', $uri, $headers, $sql);

                    yield $request;
                }
            };

            $queryResult = [];

            $pool = new Pool(
                $this->httpClient,
                $requests($query),
                [
                    'concurrency' => $concurrency,
                    'fulfilled'   => function ($response, $index) use (&$queryResult) {
                        $queryResult[$index] = true;
                    },
                    'rejected' => $this->parseReason($query),
                    'options'  => array_merge([
                        'expect' => false,
                    ], $this->options['write'] ?? []),
                ]
            );

            $promise = $pool->promise();

            try {
                $promise->wait();
            } catch (Throwable $exception) {
                foreach ($openedStreams as $openedStream) {
                    $openedStream->close();
                }

                throw $exception;
            }

            ksort($result);

            foreach ($openedStreams as $openedStream) {
                $openedStream->close();
            }

            $result[] = $queryResult;
        }

        return $result;
    }

    /**
     * @throws Throwable
     */
    public function read(array $queries, int $concurrency = 5): array
    {
        $openedStreams = [];

        $requests = function ($queries) use (&$openedStreams) {
            foreach ($queries as $index => $query) {
                /* @var Query $query */

                $params = [
                    'wait_end_of_query' => 1,
                ];

                $multipart = [
                    [
                        'name'     => 'query',
                        'contents' => $query->getQuery().' FORMAT JSON',
                    ],
                ];

                foreach ($query->getFiles() as $file) {
                    /* @var TempTable $file */
                    $tableQueryParams = $this->getTempTableQueryParams($file);

                    $stream = $file->open(false);
                    $openedStreams[] = $stream;

                    $multipart[] = [
                        'name'     => $file->getName(),
                        'contents' => $stream,
                        'filename' => $file->getName(),
                    ];

                    $params = array_merge($tableQueryParams, $params);
                }

                $body = new MultipartStream($multipart);

                $uri = $this->buildRequestUri($query->getServer(), $params, $query->getSettings());

                yield $index => new Request('POST', $uri, [], $body);
            }
        };

        $result = [];

        $pool = new Pool(
            $this->httpClient,
            $requests($queries),
            [
                'concurrency' => $concurrency,
                'fulfilled'   => function ($response, $index) use (&$result, $queries) {
                    $result[$index] = $this->assembleResult($queries[$index], $response);
                },
                'rejected' => function ($response, $index) use ($queries) {
                    $query = $queries[$index];

                    $this->parseReason($query)($response);
                },
                'options' => array_merge([
                    'expect' => false,
                ], $this->options['read'] ?? []),
            ]
        );

        $promise = $pool->promise();

        try {
            $promise->wait();
        } catch (Throwable $exception) {
            foreach ($openedStreams as $openedStream) {
                $openedStream->close();
            }

            throw $exception;
        }

        ksort($result);

        foreach ($openedStreams as $openedStream) {
            $openedStream->close();
        }

        return $result;
    }

    /**
     * Parse temp table data to append it to request.
     */
    protected function getTempTableQueryParams(TempTable $table): array
    {
        list($structure, $withColumns) = $this->assembleTempTableStructure($table);

        return [
            $table->getName().'_'.($withColumns ? 'structure' : 'types') => $structure,
            $table->getName().'_format'                                  => $table->getFormat(),
        ];
    }

    /**
     * Assembles string from TempTable structure.
     */
    protected function assembleTempTableStructure(TempTable $table): array
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
     * Determines the reason why request was rejected.
     */
    protected function parseReason(Query $query): \Closure
    {
        return function ($reason) use ($query) {
            if ($reason instanceof RequestException) {
                $response = $reason->getResponse();
                if (is_null($response)) {
                    throw TransportException::connectionError($query->getServer(), $reason->getMessage());
                } else {
                    throw TransportException::serverReturnedError($reason, $query);
                }
            }
            if ($reason instanceof ConnectException) {
                throw TransportException::connectionError($query->getServer(), $reason->getMessage());
            }

            throw $reason;
        };
    }

    /**
     * Assembles Result instance from server response.
     *
     * @throws TransportException
     */
    protected function assembleResult(Query $query, ResponseInterface $response): Result
    {
        $response = $response->getBody()->getContents();

        try {
            $result = \GuzzleHttp\json_decode($response, true);

            $statistic = new QueryStatistic(
                $result['statistics']['rows_read'] ?? 0,
                $result['statistics']['bytes_read'] ?? 0,
                $result['statistics']['elapsed'] ?? 0,
                $result['rows_before_limit_at_least'] ?? null
            );

            return new Result($query, $result['data'] ?? [], $statistic);
        } catch (\Exception $e) {
            throw TransportException::malformedResponseFromServer($response);
        }
    }

    /**
     * Builds uri with necessary params.
     */
    protected function buildRequestUri(Server $server, array $query = [], array $settings = []): string
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

        $query = array_merge($query, $settings);

        return $uri.'?'.http_build_query($query);
    }
}
