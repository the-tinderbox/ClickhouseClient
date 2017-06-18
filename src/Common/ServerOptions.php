<?php

namespace Tinderbox\Clickhouse\Common;

/**
 * Container to server options.
 */
class ServerOptions
{
    /**
     * Connection timeout.
     *
     * @var float
     */
    protected $timeout = 5.0;

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

    /**
     * Sets timeout.
     *
     * @param float $timeout
     *
     * @return \Tinderbox\Clickhouse\Common\ServerOptions
     */
    public function setTimeout(float $timeout): self
    {
        $this->timeout = $timeout;

        return $this;
    }

    /**
     * Returns timeout.
     *
     * @return float
     */
    public function getTimeout(): float
    {
        return $this->timeout;
    }
}
