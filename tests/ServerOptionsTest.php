<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\ServerOptions;

/**
 * @covers \Tinderbox\Clickhouse\Common\ServerOptions
 */
class ServerOptionsTest extends TestCase
{
    public function testServerOptions()
    {
        $options = new ServerOptions();

        $this->assertEquals(5.0, $options->getTimeout());

        $options->setTimeout(1);

        $this->assertEquals(1, $options->getTimeout());

        $this->assertEquals('http', $options->getProtocol());

        $options->setProtocol('https');

        $this->assertEquals('https', $options->getProtocol());

        $this->expectException(\TypeError::class);

        $options->setTimeout('not float');
    }
}
