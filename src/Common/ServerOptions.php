<?php

namespace Tinderbox\Clickhouse\Common;

/**
 * Container to server options.
 */
class ServerOptions
{
    /**
     * Protocol.
     *
     * @var string
     */
    protected $protocol = 'http';

    /**
     * Sets protocol.
     *
     * @param string $protocol
     *
     * @return ServerOptions
     */
    public function setProtocol(string $protocol): self
    {
        $this->protocol = $protocol;

        return $this;
    }

    /**
     * Returns protocol.
     *
     * @return string
     */
    public function getProtocol(): string
    {
        return $this->protocol;
    }
}
