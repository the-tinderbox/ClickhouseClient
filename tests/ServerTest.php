<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\ServerOptions;

/**
 * @covers \Tinderbox\Clickhouse\Server
 * @use \Tinderbox\Clickhouse\Common\ServerOptions
 */
class ServerTest extends TestCase
{
    public function testServerDefaultOptions()
    {
        $server = new Server('127.0.0.1');

        $this->assertEquals('8123', $server->getPort(), 'Sets correct default HTTP port');
        $this->assertEquals('default', $server->getDatabase(), 'Sets correct default database');
        $this->assertEquals(null, $server->getUsername(), 'Sets correct default username');
        $this->assertEquals(null, $server->getPassword(), 'Sets correct default password');
    }

    public function testGetters()
    {
        $options = (new ServerOptions())->setProtocol('https');
        $server = new Server('127.0.0.1', 8123, 'database', 'user', 'password', $options);

        $this->assertEquals('127.0.0.1', $server->getHost(), 'Sets correct host');
        $this->assertEquals('8123', $server->getPort(), 'Sets correct port');
        $this->assertEquals('database', $server->getDatabase(), 'Sets correct database');
        $this->assertEquals('user', $server->getUsername(), 'Sets correct username');
        $this->assertEquals('password', $server->getPassword(), 'Sets correct password');
        $this->assertEquals($options, $server->getOptions(), 'Sets correct options');
    }
}
