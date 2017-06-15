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

        $this->assertEquals('8123', $server->getPort());
        $this->assertEquals(5.0, $server->getOptions()->getTimeout());
    }

    public function testClickhouseGettersSetters()
    {
        $options = new ServerOptions();
        $options->setTimeout(10);

        $server = new Server('127.0.0.1', '8111', 'default', 'user', 'pass', $options);

        $this->assertEquals('127.0.0.1', $server->getHost());
        $this->assertEquals('8111', $server->getPort());
        $this->assertEquals('default', $server->getDatabase());
        $this->assertEquals('user', $server->getUsername());
        $this->assertEquals('pass', $server->getPassword());
        $this->assertEquals($options, $server->getOptions());

        $server->setHost('test.host');
        $server->setPort('8112');
        $server->setDatabase('database');
        $server->setUsername('username');
        $server->setPassword('password');

        $newOptions = new ServerOptions();
        $newOptions->setTimeout(50);

        $server->setOptions($newOptions);

        $this->assertEquals('test.host', $server->getHost());
        $this->assertEquals('8112', $server->getPort());
        $this->assertEquals('database', $server->getDatabase());
        $this->assertEquals('username', $server->getUsername());
        $this->assertEquals('password', $server->getPassword());
        $this->assertEquals($newOptions, $server->getOptions());

        $server->setDatabase(null);
        $server->setUsername(null);
        $server->setPassword(null);

        $this->assertNull($server->getDatabase());
        $this->assertNull($server->getUsername());
        $this->assertNull($server->getPassword());
    }
}
