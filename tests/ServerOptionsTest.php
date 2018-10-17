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

        $this->assertEquals('http', $options->getProtocol(), 'Sets correct default protocol');

        $options->setProtocol('https');

        $this->assertEquals('https', $options->getProtocol(), 'Sets correct protocol');
    }
}
