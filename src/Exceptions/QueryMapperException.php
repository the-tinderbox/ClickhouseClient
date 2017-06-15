<?php

namespace Tinderbox\Clickhouse\Exceptions;

class QueryMapperException extends \Exception
{
    public static function wrongBindingsNumber($inQuery, $count)
    {
        return new static('Wrong bindings count. Bindings found in query: '.$inQuery.' and given '.$count);
    }

    public static function multipleBindingsType()
    {
        return new static('Both named and unnamed bindings found');
    }
}
