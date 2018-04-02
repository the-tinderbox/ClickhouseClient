<?php

namespace Tinderbox\Clickhouse\Transport;

use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Exceptions\ClientException;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Server;

/**
 * Transport to perform queries via clickhouse cli client.
 */
class ClickhouseCLIClientTransport implements TransportInterface
{
    /**
     * Path to executable clickhouse cli client
     *
     * @var string
     */
    protected $executablePath;

    /**
     * Last execute query
     *
     * @var string
     */
    protected $lastQuery = '';

    /**
     * ClickhouseCLIClientTransport constructor.
     *
     * @param string|null $executablePath
     */
    public function __construct(string $executablePath = null)
    {
        $this->setExecutablePath($executablePath);
    }

    /**
     * Set path to client executable.
     *
     * @param string|null $executablePath
     */
    protected function setExecutablePath(string $executablePath = null)
    {
        if (is_null($executablePath)) {
            $executablePath = 'clickhouse-client';
        }

        $this->executablePath = $executablePath;
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
        $this->setLastQuery($query);

        $command = $this->buildCommandForWrite($server, $query);
        $this->executeCommand($command);

        return true;
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
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     */
    public function sendAsyncFilesWithQuery(Server $server, string $query, array $files, int $concurrency = 5): array
    {
        $result = [];

        foreach ($files as $file) {
            $this->setLastQuery($query);

            $command = $this->buildCommandForWrite($server, $query, $file);
            $this->executeCommand($command);

            $result[] = true;
        }

        return $result;
    }

    /**
     * Executes SELECT queries and returns result.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param TempTable|array|null         $tables
     *
     * @throws \Throwable
     *
     * @return \Tinderbox\Clickhouse\Query\Result
     */
    public function get(Server $server, string $query, $tables = null): Result
    {
        $this->setLastQuery($query);

        list($command, $file) = $this->buildCommandForRead($server, $query, $tables);

        try {
            $response = $this->executeCommand($command);

            if (!is_null($file)) {
                $this->removeQueryFile($file);
            }

            return $this->assembleResult($response);
        } catch (\Throwable $e) {
            if (!is_null($file)) {
                $this->removeQueryFile($file);
            }

            throw $e;
        }
    }

    /**
     * Executes async SELECT queries and returns result.
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param array                        $queries
     * @param int                          $concurrency
     *
     * @throws \Throwable
     *
     * @return array
     */
    public function getAsync(Server $server, array $queries, int $concurrency = 5): array
    {
        $result = [];

        foreach ($queries as $query) {
            $tables = $query[1] ?? null;
            $query = $query[0];

            $result[] = $this->get($server, $query, $tables);
        }

        return $result;
    }

    /**
     * Puts query in tmp file
     *
     * @param string $query
     *
     * @return string
     */
    protected function writeQueryInFile(string $query) : string
    {
        $tmpDir = sys_get_temp_dir();
        $fileName = tempnam($tmpDir, 'clickhouse_client');

        $handle = fopen($fileName, 'w');
        fwrite($handle, $query);
        fclose($handle);

        return $fileName;
    }

    /**
     * Removes tmp file with query
     *
     * @param string $fileName
     */
    protected function removeQueryFile(string $fileName)
    {
        unlink($fileName);
    }

    /**
     * Builds command for write
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param string|null                  $file
     *
     * @return string
     */
    protected function buildCommandForWrite(Server $server, string $query, string $file = null) : string
    {
        $query = escapeshellarg($query);

        $command = [];

        if (!is_null($file)) {
            $command[] = "cat ".$file.' |';
        }

        $command = array_merge($command, [
            $this->executablePath,
            "--host='{$server->getHost()}'",
            "--port='{$server->getPort()}'",
            "--database='{$server->getDatabase()}'",
            "--query={$query}"
        ]);

        return implode(' ', $command);
    }

    /**
     * Builds command to read
     *
     * @param \Tinderbox\Clickhouse\Server $server
     * @param string                       $query
     * @param null                         $tables
     *
     * @return array
     */
    protected function buildCommandForRead(Server $server, string $query, $tables = null) : array
    {
        $fileName = $this->writeQueryInFile($query);

        $command = [
            "cat {$fileName} |",
            $this->executablePath,
            "--host='{$server->getHost()}'",
            "--port='{$server->getPort()}'",
            "--database='{$server->getDatabase()}'",
            "--max_query_size=".(strlen($query) + 1024),
        ];

        if ($tables instanceof TempTable || !empty($tables)) {
            if ($tables instanceof TempTable) {
                $tables = [$tables];
            }

            foreach ($tables as $table) {
                $command = array_merge($command, $this->parseTempTable($table));
            }
        }

        return [implode(' ', $command), $fileName];
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
            "--external",
            ($withColumns ? '--structure=' : '--types=')."'{$structure}'",
            "--format='{$table->getFormat()}'",
            "--name='{$table->getName()}'",
            "--file='{$table->getSource()}'"
        ];
    }

    /**
     * Executes command and catches result or error via stdout and stderr
     *
     * @param string $command
     *
     * @return string
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     */
    protected function executeCommand(string $command) : string
    {
        $process = proc_open($command, [['pipe', 'r'], ['pipe', 'w'], ['pipe', 'w']], $pipes);

        $response = '';
        $error = '';
        $status = 1;

        if (is_resource($process)) {
            $response = stream_get_contents($pipes[1]);
            $error = stream_get_contents($pipes[2]);

            fclose($pipes[0]);
            fclose($pipes[1]);
            fclose($pipes[2]);

            $status = proc_close($process);
        }

        if ($status != 0) {
            throw ClientException::serverReturnedError($this->cleanString(str_replace(PHP_EOL, ' ', $error)).'. Query: '.$this->getLastQuery());
        }

        return $this->cleanString($response);
    }

    /**
     * Sets last executed query
     *
     * @param string $query
     */
    protected function setLastQuery(string $query)
    {
        $this->lastQuery = $query;
    }

    /**
     * Returns last executed query
     *
     * @return string
     */
    protected function getLastQuery() : string
    {
        return $this->lastQuery;
    }

    /**
     * Cleans string from tabs and spaces
     *
     * @param string $string
     *
     * @return string
     */
    protected function cleanString(string $string) : string
    {
        return trim(rtrim(ltrim($string, PHP_EOL), PHP_EOL));
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
                $preparedStructure[] = $column.' '.$type;
            }
        }

        return [implode(', ', $preparedStructure), $withColumns];
    }

    /**
     * Assembles Result instance from server response.
     *
     * @param string $response
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return \Tinderbox\Clickhouse\Query\Result
     */
    protected function assembleResult(string $response): Result
    {
        try {
            $result = json_decode($response, true);

            $statistic = new QueryStatistic(
                $result['statistics']['rows_read'] ?? 0,
                $result['statistics']['bytes_read'] ?? 0,
                $result['statistics']['elapsed'] ?? 0
            );

            return new Result($result['data'] ?? [], $statistic);
        } catch (\Throwable $e) {
            throw ClientException::serverReturnedError($response);
        }
    }
}
