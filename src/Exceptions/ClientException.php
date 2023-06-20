<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class ClientException extends \Exception
{
    public static function clusterIsNotProvided(): static
    {
        return new static('Can not execute query using specified server because cluster is not provided');
    }

    public static function clusterExists(string $name): static
    {
        return new static('Can not add cluster with name ['.$name.'], because it already added');
    }
}
