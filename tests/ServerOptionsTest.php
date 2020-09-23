<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\ServerOptions;

/**
 * @covers \Tinderbox\Clickhouse\Common\ServerOptions
 */
class ServerOptionsTest extends TestCase
{
    public function testProtocolFromServerOptions()
    {
        $options = new ServerOptions();

        $this->assertEquals('http', $options->getProtocol(), 'Sets correct default protocol');

        $options->setProtocol('https');

        $this->assertEquals('https', $options->getProtocol(), 'Sets correct protocol');
    }

    public function testTagsFromServerOptions()
    {
        $options = new ServerOptions();

        $options->addTag('test');

        $this->assertTrue(
            in_array('test', $options->getTags(), true),
            'Sets correct tags'
        );

        $options->addTag('tag');

        $this->assertTrue(
            in_array('test', $options->getTags(), true),
            'Sets correct tags'
        );
        $this->assertTrue(
            in_array('tag', $options->getTags(), true),
            'Sets correct tags'
        );

        $options->setTags(['other']);

        $this->assertTrue(
            in_array('other', $options->getTags(), true),
            'Sets correct tags'
        );

        $options->addTag('tag');

        $this->assertTrue(
            in_array('other', $options->getTags(), true),
            'Sets correct tags'
        );
        $this->assertTrue(
            in_array('tag', $options->getTags(), true),
            'Sets correct tags'
        );
    }
}
