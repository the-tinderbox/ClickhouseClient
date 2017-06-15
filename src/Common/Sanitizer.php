<?php

namespace Tinderbox\Clickhouse\Common;

/**
 * Helper to escape value before it bound in query.
 */
abstract class Sanitizer
{
    /**
     * Escapes value.
     *
     * @param mixed $value
     *
     * @return mixed
     */
    public static function escape($value)
    {
        if (is_string($value)) {
            $value = addslashes($value);
            $value = "'{$value}'";
        }

        return $value;
    }
}
