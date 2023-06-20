<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class ResultException extends \Exception
{
    public static function propertyNotExists($name): static
    {
        return new static('Query result has no property '.$name);
    }

    public static function isReadonly(): static
    {
        return new static('Query result is read-only');
    }
}
