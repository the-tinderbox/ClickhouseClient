<?php

namespace Tinderbox\Clickhouse\Transport;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\MultipartStream;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Stream;
use Psr\Http\Message\ResponseInterface;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\TransportException;
use Tinderbox\Clickhouse\Interfaces\FileInterface;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\Common\Format;
use GuzzleHttp\RequestOptions;

/**
 * Http transport to perform queries.
 */
class HttpTransport implements TransportInterface
{
    const SUPPORTED_READ_FORMATS = [Format::JSON, Format::JSONCompact, Format::TSV, Format::CSV];

    /**
     * GuzzleClient.
     *
     * @var Client
     */
    protected $httpClient;

    /**
     * Array with two keys (read and write) with guzzle options for corresponding requests.
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
     * ]
     *
     * @var array
     */
    private $options;

    /**
     * HttpTransport constructor.
     *
     * @param Client $client
     * @param array $options
     */
    public function __construct(Client $client = null, array $options = [])
    {
        $this->setClient($client);

        $this->options = $options;
    }

    /**
     * Returns default headers for requests.
     *
     * @return array
     */
    protected function getHeaders()
    {
        return [
            'Accept-Encoding' => 'gzip',
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
     * Executes write queries.
     *
     * @param array $queries
     * @param int   $concurrency
     *
     * @return array
     * @throws \Throwable
     */
    public function write(array $queries, int $concurrency = 5) : array
    {
        $result = [];
        $openedStreams = [];

        foreach ($queries as $query) {
            $requests = function(Query $query) use(&$openedStreams) {
                if (!empty($query->getFiles())) {
                    foreach ($query->getFiles() as $file) {
                        /* @var FileInterface $file */
                        $headers = $this->getHeaders();

                        $uri = $this->buildRequestUri($query->getServer(), [
                            'query' => $query->getQuery()
                        ], $query->getSettings());

                        $stream = $file->open();
                        $openedStreams[] = $stream;

                        $request = new Request('POST', $uri, $headers, $stream);

                        yield $request;
                    }
                } else {
                    $headers = $this->getHeaders();

                    $uri = $this->buildRequestUri($query->getServer(), [], $query->getSettings());

                    $request = new Request('POST', $uri, $headers, gzencode($query->getQuery()));

                    yield $request;
                }
            };

            $queryResult = [];

            $pool = new Pool(
                $this->httpClient, $requests($query), [
                    'concurrency' => $concurrency,
                    'fulfilled' => function ($response, $index) use (&$queryResult, $query) {
                        $queryResult[$index] = true;
                    },
                    'rejected' => $this->parseReason($query),
                    'options' => array_merge([
                        'expect' => false
                    ], $this->options['write'] ?? []),
                ]
            );

            $promise = $pool->promise();

            try {
                $promise->wait();
            } catch (\Throwable $exception) {
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
     * @param array $queries
     * @param int $concurrency
     * @return Result[]
     * @throws \Throwable
     */
    public function read(array $queries, int $concurrency = 5) : array
    {
        foreach ($queries as $query) {
            if (!in_array($query->getFormat(), self::SUPPORTED_READ_FORMATS)) {
                throw TransportException::unsupportedFormat($query->getFormat(), self::SUPPORTED_READ_FORMATS);
            }
        }

        $openedStreams = [];
        $resources = [];

        $requests = function ($queries) use(&$openedStreams, &$resources) {
            foreach ($queries as $index => $query) {
                /* @var Query $query */

                $params = [
                    'wait_end_of_query' => 1,
                ];

                if ($query->getFiles()) {
                    $multipart = [
                        [
                            'name'     => 'query',
                            'contents' => $query->getQuery(),
                        ]
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
                } else {
                    $body = $query->getQuery();
                }

                $uri = $this->buildRequestUri($query->getServer(), $params, $query->getSettings());

                $resources[$index] = fopen('php://temp', 'r+');
                $client = $this->httpClient;

                yield $index => static function () use ($client, $uri, $resources, $body, $index) {
                     return $client->postAsync($uri, [
                        RequestOptions::SINK => $resources[$index],
                        RequestOptions::BODY => $body
                    ]);
                };
            }
        };

        $result = [];

        $pool = new Pool(
            $this->httpClient, $requests($queries), [
                'concurrency' => $concurrency,
                'fulfilled' => function (ResponseInterface $response, $index) use (&$result, $queries, &$resources) {
                    $result[$index] = $this->assembleResult($queries[$index], $response, $resources[$index]);
                },
                'rejected' => function ($response, $index) use ($queries) {
                    $query = $queries[$index];

                    $this->parseReason($query)($response);
                },
                'options' => array_merge([
                    'expect' => false
                ], $this->options['read'] ?? []),
            ]
        );

        $promise = $pool->promise();

        try {
            $promise->wait();
        } catch (\Throwable $exception) {
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
     *
     * @param \Tinderbox\Clickhouse\Common\TempTable $table
     *
     * @return array
     */
    protected function getTempTableQueryParams(TempTable $table)
    {
        list($structure, $withColumns) = $this->assembleTempTableStructure($table);

        return [
            $table->getName() . '_' . ($withColumns ? 'structure' : 'types') => $structure,
            $table->getName() . '_format' => $table->getFormat(),
        ];
    }

    /**
     * Assembles string from TempTable structure.
     *
     * @param \Tinderbox\Clickhouse\Common\TempTable $table
     *
     * @return array
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
                $preparedStructure[] = $column . ' ' . $type;
            }
        }

        return [implode(', ', $preparedStructure), $withColumns];
    }

    /**
     * Determines the reason why request was rejected.
     *
     * @param Query $query
     *
     * @return \Closure
     */
    protected function parseReason(Query $query)
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

            throw $reason;
        };
    }

    /**
     * Assembles Result instance from server response.
     *
     * @param Query $query
     * @param \Psr\Http\Message\ResponseInterface $response
     *
     * @param $resource
     * @return \Tinderbox\Clickhouse\Query\Result
     * @throws TransportException
     */
    protected function assembleResult(Query $query, ResponseInterface $response, $resource): Result
    {
        $result = [];

        /** @var Stream $stream */
        $stream = $response->getBody();

        try {
            switch ($query->getFormat()) {
                case Format::JSON:
                case Format::JSONCompact:
                    $result = \GuzzleHttp\json_decode($stream->getContents(), true);

                    $statistic = new QueryStatistic(
                        $result['statistics']['rows_read'] ?? 0,
                        $result['statistics']['bytes_read'] ?? 0,
                        $result['statistics']['elapsed'] ?? 0,
                        $result['rows_before_limit_at_least'] ?? null
                    );

                    $meta = $this->assembleMeta($result);
                    break;
                case Format::CSV:
                    while ($row = fgetcsv($resource)) {
                        $result['data'][] = $row;
                    }

                    $summary = $response->getHeader('X-ClickHouse-Summary');

                    if (!empty($summary)) {
                        $stats = \GuzzleHttp\json_decode($summary[0], true);
                    } else {
                        $stats = [
                            'read_rows'  => 0,
                            'read_bytes' => 0,
                        ];
                    }

                    $statistic = new QueryStatistic($stats['read_rows'], $stats['read_bytes'], 0, null);
                    $meta      = new Query\Meta();
                    break;
                case Format::TSV:
                    while ($row = fgetcsv($resource, 0, "\t")) {
                        $result['data'][] = $row;
                    }

                    $summary = $response->getHeader('X-ClickHouse-Summary');

                    if (!empty($summary)) {
                        $stats = \GuzzleHttp\json_decode($summary[0], true);
                    } else {
                        $stats = [
                            'read_rows'  => 0,
                            'read_bytes' => 0,
                        ];
                    }

                    $statistic = new QueryStatistic($stats['read_rows'], $stats['read_bytes'], 0, null);
                    $meta      = new Query\Meta();
                    break;
            }

            return new Result($query, $result['data'] ?? [], $statistic, $meta);
        } catch (\Exception $e) {
            $stream->rewind();

            throw TransportException::malformedResponseFromServer($stream->getContents());
        }
    }

    /**
     * @param array $response
     * @return Query\Meta
     */
    protected function assembleMeta(array $response): Query\Meta
    {
        $meta = new Query\Meta();
        foreach ($response['meta'] as $row) {
            $meta->push(new Query\MetaColumn($row['name'], $row['type']));
        }

        return $meta;
    }

    /**
     * Builds uri with necessary params.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param array                        $query
     * @param array                        $settings
     *
     * @return string
     */
    protected function buildRequestUri(Server $server, array $query = [], array $settings = []): string
    {
        $uri = $server->getOptions()->getProtocol() . '://' . $server->getHost() . ':' . $server->getPort();

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

        return $uri . '?' . http_build_query($query);
    }
}
