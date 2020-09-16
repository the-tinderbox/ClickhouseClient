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
     * Tags.
     *
     * @var array
     */
    protected $tags = [];

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
     * Set tags.
     *
     * @param array $tags
     *
     * @return ServerOptions
     */
    public function setTags(array $tags): self
    {
        $this->tags = [];

        foreach ($tags as $tag) {
            $this->addTag($tag);
        }

        return $this;
    }

    /**
     * Adds tag.
     *
     * @param string $tag
     *
     * @return ServerOptions
     */
    public function addTag(string $tag): self
    {
        $this->tags[$tag] = true;

        return $this;
    }

    /**
     * Returns tags.
     *
     * @return array
     */
    public function getTags(): array
    {
        return array_keys($this->tags);
    }
}
