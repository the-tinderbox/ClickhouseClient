<?php

namespace Tinderbox\Clickhouse\Exceptions;

class ClusterException extends \Exception
{
    public static function missingServerHostname()
    {
        return new static('Each server in cluster must have specified hostname as an array key');
    }

    public static function serverHostnameDuplicate($hostname)
    {
        return new static('Hostname '.$hostname.' already provided');
    }

    public static function invalidServerProvided($server)
    {
        return new static(
            'Invalid server provided. Server must be the type of Server, but '.gettype(
                $server
            ).' given'
        );
    }

    public static function serverNotFound($hostname)
    {
        return new static('Server with hostname '.$hostname.' is not found in cluster');
    }
}
