<?php

namespace Tinderbox\Clickhouse\Common;

use GuzzleHttp\Psr7\Stream;
use Psr\Http\Message\StreamInterface;
use Tinderbox\Clickhouse\Interfaces\FileInterface;

class FileFromString extends AbstractFile implements FileInterface
{
    public function open(bool $gzip = true) : StreamInterface
    {
        $handle = fopen('php://memory', 'r+');
        fwrite($handle, $gzip ? gzencode($this->source) : $this->source);
        fseek($handle, 0);

        return new Stream($handle);
    }
}