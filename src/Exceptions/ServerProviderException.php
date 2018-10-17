<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class ServerProviderException extends \Exception
{
    public static function clusterExists(string $name)
    {
        return new static('Can not add cluster with name ['.$name.'], because it already added');
    }

    public static function clusterNotFound(string $name)
    {
        return new static('Can not find cluster with name ['.$name.']');
    }

    public static function serverHostnameDuplicate($hostname)
    {
        return new static('Server with hostname ['.$hostname.'] already provided');
    }

    public static function serverHostnameNotFound($hostname)
    {
        return new static('Can not find server with hostname ['.$hostname.']');
    }
}
