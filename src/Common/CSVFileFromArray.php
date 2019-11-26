<?php declare( strict_types=1 );

namespace Tinderbox\Clickhouse\Common;

use Tinderbox\Clickhouse\Interfaces\FileInterface;
use Psr\Http\Message\StreamInterface;
use GuzzleHttp\Psr7\Stream;

class CSVFileFromArray implements FileInterface
{
    private $resource;

    public function __construct(array $rows, ?array $headers = null, string $escape = "\\")
    {
        $this->resource = fopen('php://memory', 'r+');
        if ($headers !== null) {
            fputcsv($this->resource, $headers, ',', '"', $escape);
        }

        foreach ($rows as $row) {
            fputcsv($this->resource, $row, ',', '"', $escape);
        }
        rewind($this->resource);
    }

    public function open(bool $gzip = true): StreamInterface
    {
        return new Stream($this->resource);
    }
}
