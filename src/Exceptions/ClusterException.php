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
}
