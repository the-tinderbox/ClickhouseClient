<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Common\ServerOptions;
use Tinderbox\Clickhouse\Exceptions\ClusterException;

/**
 * @covers \Tinderbox\Clickhouse\Cluster
 * @use \Tinderbox\Clickhouse\Exceptions\ClusterException
 * @use \Tinderbox\Clickhouse\Common\ServerOptions
 */
class ClusterTest extends TestCase
{
    public function testGetters()
    {
        $servers = [
            new Server('127.0.0.1'),
            new Server('127.0.0.2'),
        ];

        $cluster = new Cluster('test_cluster', $servers);

        $this->assertEquals('test_cluster', $cluster->getName(), 'Return correct cluster name passed in constructor');
        $this->assertEquals($servers[0], $cluster->getServers()[$servers[0]->getHost()], 'Return correct cluster structure');
        $this->assertEquals($servers[1], $cluster->getServers()[$servers[1]->getHost()], 'Return correct cluster structure');

        $this->assertEquals($servers[0], $cluster->getServerByHostname('127.0.0.1'), 'Return correct server by hostname');
        $this->assertEquals($servers[1], $cluster->getServerByHostname('127.0.0.2', 'Return correct server by hostname'));

        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage('Server with hostname [unknown_hostname] is not found in cluster');
        $cluster->getServerByHostname('unknown_hostname');
    }

    public function testServersWithAliases()
    {
        $servers = [
            new Server('127.0.0.1'),
            'aliased' => new Server('127.0.0.2'),
        ];

        $cluster = new Cluster('test_cluster', $servers);

        $this->assertEquals($servers[0], $cluster->getServers()[$servers[0]->getHost()], 'Return correct cluster structure');
        $this->assertEquals($servers['aliased'], $cluster->getServers()['aliased'], 'Return correct cluster structure');

        $this->assertEquals($servers[0], $cluster->getServerByHostname('127.0.0.1'), 'Return correct server by hostname');
        $this->assertEquals($servers['aliased'], $cluster->getServerByHostname('aliased', 'Return correct server by hostname'));
    }

    public function testServerFromArray()
    {
        $server = [
            'host'     => '127.0.0.2',
            'port'     => '123',
            'database' => 'not_default',
            'username' => 'user',
            'password' => 'secret',
            'options'  => (new ServerOptions())->setProtocol('https'),
        ];

        $cluster = new Cluster('test_cluster', [
            $server,
        ]);

        $createdServer = $cluster->getServerByHostname('127.0.0.2');

        $this->assertEquals($server['host'], $createdServer->getHost(), 'Correctly passes server host to server constructor from array');
        $this->assertEquals($server['port'], $createdServer->getPort(), 'Correctly passes server port to server constructor from array');
        $this->assertEquals($server['database'], $createdServer->getDatabase(), 'Correctly passes server database to server constructor from array');
        $this->assertEquals($server['username'], $createdServer->getUsername(), 'Correctly passes server username to server constructor from array');
        $this->assertEquals($server['password'], $createdServer->getPassword(), 'Correctly passes server password to server constructor from array');
        $this->assertEquals($server['options'], $createdServer->getOptions(), 'Correctly passes server options to server constructor from array');
    }

    public function testServersDuplicates()
    {
        $servers = [
            new Server('127.0.0.1'),
            new Server('127.0.0.1'),
        ];

        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage('Hostname [127.0.0.1] already provided');

        new Cluster('test_cluster', $servers);
    }

    public function testServersWithTags()
    {
        $server = [
            'host'     => '127.0.0.1',
            'port'     => 8123,
            'database' => 'default',
            'username' => 'default',
            'password' => '',
            'options'  => (new ServerOptions())->addTag('tag'),
        ];

        $cluster = new Cluster('test', [
            $server,
        ]);

        $servers = $cluster->getServersByTag('tag');

        $this->assertEquals(array_keys($servers)[0], $server['host']);
    }

    public function testServerTagNotFound()
    {
        $cluster = new Cluster('test', []);

        $this->expectException(ClusterException::class);
        $this->expectExceptionMessage('There are no servers with tag [tag] in cluster');

        $cluster->getServersByTag('tag');
    }
}
