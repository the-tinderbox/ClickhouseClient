<?php

namespace Tinderbox\Clickhouse\Common;

use GuzzleHttp\Psr7\Stream;
use Psr\Http\Message\StreamInterface;
use Tinderbox\Clickhouse\Interfaces\FileInterface;
use Tinderbox\Clickhouse\Support\CcatStream;

class MergedFiles extends File implements FileInterface
{
    protected function getCcatPath(): string
    {
        return __DIR__.DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.'bin'.DIRECTORY_SEPARATOR.'ccat_'.strtolower(PHP_OS);
    }

    public function open(bool $gzip = true): StreamInterface
    {
        $descriptorspec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open($this->getCcatPath().' '.escapeshellarg($this->source), $descriptorspec, $pipes);
        $stream = $pipes[1];

        if ($gzip) {
            stream_filter_append($stream, 'zlib.deflate', STREAM_FILTER_READ, ['window' => 30]);
        }

        return new CcatStream(new Stream($stream), $process);
    }
}
