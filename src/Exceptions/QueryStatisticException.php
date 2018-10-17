<?php

namespace Tinderbox\Clickhouse\Exceptions;

/**
 * @codeCoverageIgnore
 */
class QueryStatisticException extends \Exception
{
    public static function propertyNotExists($name)
    {
        return new static('Query statistic has no property '.$name);
    }
}
