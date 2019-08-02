<?php

namespace Tinderbox\Clickhouse\Support;

use Tinderbox\Clickhouse\Server;

trait ServerTrait
{
    protected function getServer($database = 'default', $username = 'default', $password = null) : Server
    {
        return new Server(getenv('CH_HOST'), getenv('CH_PORT'), $database, $username, $password);
    }
}
