<?php

namespace Tinderbox\Clickhouse\Common;

use GuzzleHttp\Psr7\Stream;
use Psr\Http\Message\StreamInterface;
use Tinderbox\Clickhouse\Interfaces\FileInterface;

class File extends AbstractFile implements FileInterface
{
    public function open(bool $gzip = true): StreamInterface
    {
        $handle = fopen($this->getSource(), 'r');

        if ($gzip) {
            stream_filter_append($handle, 'zlib.deflate', STREAM_FILTER_READ, ['window' => 30]);
        }

        return new Stream($handle);
    }
}
