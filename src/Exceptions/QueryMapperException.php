<?php

namespace Tinderbox\Clickhouse\Exceptions;

class QueryMapperException extends \Exception
{
    public static function multipleBindingsType()
    {
        return new static('Both named and unnamed bindings found');
    }
}
