<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class ClusterException extends \Exception
{
    public static function missingServerHostname()
    {
        return new static('Each server in cluster must have specified hostname as an array key');
    }

    public static function serverHostnameDuplicate($hostname)
    {
        return new static('Hostname ['.$hostname.'] already provided');
    }

    public static function serverNotFound($hostname)
    {
        return new static('Server with hostname ['.$hostname.'] is not found in cluster');
    }

    public static function tagNotFound($tag)
    {
        return new static('There are no servers with tag ['.$tag.'] in cluster');
    }

    public static function serverNotFoundByTag($tag, $hostname)
    {
        return new static('Server with hostname ['.$hostname.'] and tag ['.$tag.'] is not found in cluster');
    }
}
