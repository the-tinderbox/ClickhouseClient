<?php

namespace Tinderbox\Clickhouse\Exceptions;

class QueryStatisticException extends \Exception
{
    public static function propertyNotExists($name)
    {
        return new static('Query statistic has no property '.$name);
    }
}
