<?php

namespace Tinderbox\Clickhouse\Support;

use GuzzleHttp\Psr7\StreamDecoratorTrait;
use Psr\Http\Message\StreamInterface;

class CcatStream implements StreamInterface
{
    use StreamDecoratorTrait;

    public function __construct(
        private StreamInterface $stream,
        protected mixed $process
    ) {}

    public function seek($offset, $whence = SEEK_SET): void
    {
        throw new \RuntimeException('Cannot seek a NoSeekStream');
    }

    public function isSeekable(): bool
    {
        return false;
    }

    public function getSize(): ?int {
        return null;
    }
}
