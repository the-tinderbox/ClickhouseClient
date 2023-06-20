<?php

namespace Tinderbox\Clickhouse\Common;

/**
 * Helper to escape value before it bound in query.
 */
abstract class Sanitizer
{
    /**
     * Escapes value.
     */
    public static function escape(mixed $value): mixed
    {
        if (is_string($value)) {
            $value = addslashes($value);
            $value = "'{$value}'";
        }

        return $value;
    }
}
