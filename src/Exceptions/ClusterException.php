<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class ClusterException extends \Exception
{
    public static function missingServerHostname(): static
    {
        return new static('Each server in cluster must have specified hostname as an array key');
    }

    public static function serverHostnameDuplicate($hostname): static
    {
        return new static('Hostname ['.$hostname.'] already provided');
    }

    public static function serverNotFound($hostname): static
    {
        return new static('Server with hostname ['.$hostname.'] is not found in cluster');
    }

    public static function tagNotFound($tag): static
    {
        return new static('There are no servers with tag ['.$tag.'] in cluster');
    }

    public static function serverNotFoundByTag($tag, $hostname): static
    {
        return new static('Server with hostname ['.$hostname.'] and tag ['.$tag.'] is not found in cluster');
    }
}
