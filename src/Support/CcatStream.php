<?php

namespace Tinderbox\Clickhouse\Support;

use GuzzleHttp\Psr7\NoSeekStream;
use Psr\Http\Message\StreamInterface;

class CcatStream extends NoSeekStream
{
    protected $process;

    /**
     * CcatStream constructor.
     *
     * @param \Psr\Http\Message\StreamInterface $stream
     * @param                                   $process
     */
    public function __construct(StreamInterface $stream, $process)
    {
        parent::__construct($stream);

        $this->process = $process;
    }

    public function getSize()
    {
    }
}
