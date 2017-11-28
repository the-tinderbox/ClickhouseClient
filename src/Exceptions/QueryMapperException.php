<?php

namespace Tinderbox\Clickhouse\Exceptions;

class QueryMapperException extends \Exception
{
    public static function wrongBindingsNumber(int $inQuery, int $count): self
    {
        return new static('Wrong bindings count. Bindings found in query: '.$inQuery.' and given '.$count);
    }

    public static function multipleBindingsType(): self
    {
        return new static('Both named and unnamed bindings found');
    }
}
