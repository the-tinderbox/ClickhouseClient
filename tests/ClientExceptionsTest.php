<?php

namespace Tinderbox\Clickhouse;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Exceptions\ClientException;

/**
 * @covers \Tinderbox\Clickhouse\Exceptions\ClientException
 */
class ClientExceptionsTest extends TestCase
{
    public function testInvalidServerProvided(): void
    {
        $e = ClientException::invalidServerProvided('null');

        $this->assertInstanceOf(ClientException::class, $e);
    }

    public function testClusterIsNotProvided(): void
    {
        $e = ClientException::clusterIsNotProvided();

        $this->assertInstanceOf(ClientException::class, $e);
    }

    public function testConnectionError(): void
    {
        $e = ClientException::connectionError();

        $this->assertInstanceOf(ClientException::class, $e);
    }

    public function testServerReturnedError(): void
    {
        $e = ClientException::serverReturnedError('Syntax error');

        $this->assertInstanceOf(ClientException::class, $e);
    }

    public function testMalformedResponseFromServer(): void
    {
        $e = ClientException::malformedResponseFromServer('some text');

        $this->assertInstanceOf(ClientException::class, $e);
    }

    public function testInsertFileNotFound(): void
    {
        $e = ClientException::insertFileNotFound('/tmp/test.csv');

        $this->assertInstanceOf(ClientException::class, $e);
    }
}
