<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class ResultException extends \Exception
{
    public static function propertyNotExists($name)
    {
        return new static('Query result has no property '.$name);
    }

    public static function isReadonly()
    {
        return new static('Query result is read-only');
    }
}
