<?php

namespace Tinderbox\Clickhouse;

use Tinderbox\Clickhouse\Common\ServerOptions;

/**
 * Alone server credentials.
 */
class Server
{
    /**
     * Host.
     */
    protected string $host;

    /**
     * Port.
     */
    protected string $port;

    /**
     * Database.
     */
    protected ?string $database;

    /**
     * Username.
     */
    protected ?string $username;

    /**
     * Password.
     */
    protected ?string $password;

    /**
     * Options.
     */
    protected ?ServerOptions $options;

    /**
     * Server constructor.
     */
    public function __construct(
        string $host,
        string $port = '8123',
        ?string $database = 'default',
        ?string $username = null,
        ?string $password = null,
        ?ServerOptions $options = null
    ) {
        $this->setHost($host);
        $this->setPort($port);
        $this->setDatabase($database);
        $this->setUsername($username);
        $this->setPassword($password);
        $this->setOptions($options);
    }

    /**
     * Sets host.
     */
    public function setHost(string $host): self
    {
        $this->host = $host;

        return $this;
    }

    /**
     * Returns host.
     */
    public function getHost(): string
    {
        return $this->host;
    }

    /**
     * Sets port.
     */
    public function setPort(string $port): self
    {
        $this->port = $port;

        return $this;
    }

    /**
     * Returns port.
     */
    public function getPort(): string
    {
        return $this->port;
    }

    /**
     * Sets database.
     */
    public function setDatabase(?string $database = null): self
    {
        $this->database = $database;

        return $this;
    }

    /**
     * Returns database.
     */
    public function getDatabase(): ?string
    {
        return $this->database;
    }

    /**
     * Sets username.
     */
    public function setUsername(?string $username = null): self
    {
        $this->username = $username;

        return $this;
    }

    /**
     * Returns username.
     */
    public function getUsername(): ?string
    {
        return $this->username;
    }

    /**
     * Sets password.
     */
    public function setPassword(?string $password = null): self
    {
        $this->password = $password;

        return $this;
    }

    /**
     * Returns password.
     */
    public function getPassword(): ?string
    {
        return $this->password;
    }

    /**
     * Sets options.
     *
     * If no options provided, will use default options
     */
    public function setOptions(?ServerOptions $options = null): self
    {
        if (is_null($options)) {
            return $this->setDefaultOptions();
        }

        $this->options = $options;

        return $this;
    }

    /**
     * Sets default options.
     */
    protected function setDefaultOptions(): self
    {
        $this->options = new ServerOptions();

        return $this;
    }

    /**
     * Returns options.
     */
    public function getOptions(): ServerOptions
    {
        return $this->options;
    }
}
