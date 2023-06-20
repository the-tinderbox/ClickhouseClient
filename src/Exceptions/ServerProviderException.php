<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class ServerProviderException extends \Exception
{
    public static function clusterExists(string $name): static
    {
        return new static('Can not add cluster with name ['.$name.'], because it already added');
    }

    public static function clusterNotFound(string $name): static
    {
        return new static('Can not find cluster with name ['.$name.']');
    }

    public static function serverHostnameDuplicate($hostname): static
    {
        return new static('Server with hostname ['.$hostname.'] already provided');
    }

    public static function serverHostnameNotFound($hostname): static
    {
        return new static('Can not find server with hostname ['.$hostname.']');
    }

    public static function serverTagNotFound($tag): static
    {
        return new static('Can not find servers with tag ['.$tag.']');
    }

    public static function serverHostnameNotFoundForTag($tag, $hostname): static
    {
        return new static('Can not find servers with hostname ['.$hostname.'] and tag ['.$tag.']');
    }
}
