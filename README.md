# Clickhouse Client
[![Build Status](https://travis-ci.org/the-tinderbox/ClickhouseClient.svg?branch=master&200)](https://travis-ci.org/the-tinderbox/ClickhouseClient) [![Coverage Status](https://coveralls.io/repos/github/the-tinderbox/ClickhouseClient/badge.svg?branch=master&200)](https://coveralls.io/github/the-tinderbox/ClickhouseClient?branch=master)

Package was written as client for [Clickhouse](https://clickhouse.yandex/).

Client uses [Guzzle](https://github.com/guzzle/guzzle) for sending Http requests to Clickhouse servers.

## Requirements
`php7.1`

## Install

Composer

```bash
composer require the-tinderbox/clickhouse-php-client
```

# Usage

Client works with alone server and cluster. Also, client can make async select and insert (from local files) queries.

## Alone server

```php
$server = new Tinderbox\Clickhouse\Server('127.0.0.1', '8123', 'default', 'user', 'pass');
$serverProvider = (new Tinderbox\Clickhouse\ServerProvider())->addServer($server);

$client = new Tinderbox\Clickhouse\Client($serverProvider);
```

## Cluster

```php
$testCluster = new Tinderbox\Clickhouse\Cluster('cluster-name', [
    'server-1' => [
        'host' => '127.0.0.1',
        'port' => '8123',
        'database' => 'default',
        'user' => 'user',
        'password' => 'pass'
    ],
    'server-2' => new Tinderbox\Clickhouse\Server('127.0.0.1', '8124', 'default', 'user', 'pass')
]);

$anotherCluster = new Tinderbox\Clickhouse\Cluster('cluster-name', [
    [
        'host' => '127.0.0.1',
        'port' => '8125',
        'database' => 'default',
        'user' => 'user',
        'password' => 'pass'
    ],
    new Tinderbox\Clickhouse\Server('127.0.0.1', '8126', 'default', 'user', 'pass')
]);

$serverProvider = (new Tinderbox\Clickhouse\ServerProvider())->addCluster($testCluster)->addCluster($anotherCluster);

$client = (new Tinderbox\Clickhouse\Client($serverProvider));
```

Before execute any query on cluster, you should provide cluster name and client will run all queries on specified cluster.

```
$client->onCluster('test-cluster');
```

By default client will use random server in given list of servers or in specified cluster. If you want to perform request on specified server you should use
`using($hostname)` method on client and then run query. Client will remember hostname for next queries:

```php
$client->using('server-2')->select('select * from table');
```

## Server tags

```php
$firstServerOptionsWithTag = (new \Tinderbox\Clickhouse\Common\ServerOptions())->setTag('tag');
$secondServerOptionsWithAnotherTag = (new \Tinderbox\Clickhouse\Common\ServerOptions())->setTag('another-tag');

$server = new Tinderbox\Clickhouse\Server('127.0.0.1', '8123', 'default', 'user', 'pass', $firstServerOptionsWithTag);

$cluster = new Tinderbox\Clickhouse\Cluster('cluster', [
    new Tinderbox\Clickhouse\Server('127.0.0.2', '8123', 'default', 'user', 'pass', $secondServerOptionsWithAnotherTag)
]);

$serverProvider = (new Tinderbox\Clickhouse\ServerProvider())->addServer($server)->addCluster($cluster);

$client = (new Tinderbox\Clickhouse\Client($serverProvider));
```

To use server with tag, you should call ```usingServerWithTag``` function before execute any query.

```php
$client->usingServerWithTag('tag');
```

## Select queries

Any SELECT query will return instance of `Result`. This class implements interfaces `\ArrayAccess`, `\Countable` и `\Iterator`,
which means that it can be used as an array.

Array with result rows can be obtained via `rows` property

```php
$rows = $result->rows;
$rows = $result->getRows();
```

Also you can get some statistic of your query execution:

1. Number of read rows
2. Number of read bytes
3. Time of query execution
4. Rows before limit at least

Statistic can be obtained via `statistic` property

```php
$statistic = $result->statistic;
$statistic = $result->getStatistic();

echo $statistic->rows;
echo $statistic->getRows();

echo $statistic->bytes;
echo $statistic->getBytes();

echo $statistic->time;
echo $statistic->getTime();

echo $statistic->rowsBeforeLimitAtLeast;
echo $statistic->getRowsBeforeLimitAtLeast();
```

### Sync

```php
$result = $client->readOne('select number from system.numbers limit 100');

foreach ($result as $number) {
    echo $number['number'].PHP_EOL;
}
```

**Using local files**

You can use local files as temporary tables in Clickhouse. You should pass as third argument array of `TempTable` instances.
instance.

In this case will be sent one file to the server from which Clickhouse will extract data to temporary table.
Structure of table will be:

* number - UInt64

If you pass such an array as a structure:

```php
['UInt64']
```

Then each column from file wil be named as _1, _2, _3.

```php
$result = $client->readOne('select number from system.numbers where number in _numbers limit 100', new TempTable('_numbers', 'numbers.csv', [
    'number' => 'UInt64'
]));

foreach ($result as $number) {
    echo $number['number'].PHP_EOL;
}
```

You can provide path to file or pass `FileInterface` instance as second argument.

There is some other types of file streams which could be used to send to server:
* File - simple file stored on disk;
* FileFromString - stream created from string. For example: `new FileFromString('1'.PHP_EOL.'2'.PHP_EOL.'3'.PHP_EOL)`
* MergedFiles - stream which includes many files and merges them all in one.
You should pass to constructor file path, which contains list of files which
should be megred in one stream.
* TempTable - wrapper to any of `FileInterface` instance and contains structure. Usefull
to make inserts using with `MergedFiles`.

### Async

Unlike the `readOne` method, which returns` Result`, the `read` method returns an array of` Result` for each executed query.

```php

list($clicks, $visits, $views) = $client->read([
    ['query' => "select * from clicks where date = '2017-01-01'"],
    ['query' => "select * from visits where date = '2017-01-01'"],
    ['query' => "select * from views where date = '2017-01-01'"],
]);

foreach ($clicks as $click) {
    echo $click['date'].PHP_EOL;
}

```
**In `read` method, you can pass the parameter `$concurrency` which is responsible for the maximum simultaneous number of requests.**

**Using local files**

As with synchronous select request you can pass files to the server:

```php

list($clicks, $visits, $views) = $client->read([
    ['query' => "select * from clicks where date = '2017-01-01' and userId in _users", new TempTable('_users', 'users.csv', ['number' => 'UInt64'])],
    ['query' => "select * from visits where date = '2017-01-01'"],
    ['query' => "select * from views where date = '2017-01-01'"],
]);

foreach ($clicks as $click) {
    echo $click['date'].PHP_EOL;
}

```

With asynchronous requests you can pass multiple files as with synchronous request.

## Insert queries

Insert queries always returns true or throws exceptions in case of error.

Data can be written row by row or from local CSV or TSV files.

```php
$client->writeOne("insert into table (date, column) values ('2017-01-01',1), ('2017-01-02',2)");
$client->write([
    ['query' => "insert into table (date, column) values ('2017-01-01',1), ('2017-01-02',2)"],
    ['query' => "insert into table (date, column) values ('2017-01-01',1), ('2017-01-02',2)"],
    ['query' => "insert into table (date, column) values ('2017-01-01',1), ('2017-01-02',2)"]
]);

$client->writeFiles('table', ['date', 'column'], [
    new Tinderbox\Clickhouse\Common\File('/file-1.csv'),
    new Tinderbox\Clickhouse\Common\File('/file-2.csv')
]);

$client->insertFiles('table', ['date', 'column'], [
    new Tinderbox\Clickhouse\Common\File('/file-1.tsv'),
    new Tinderbox\Clickhouse\Common\File('/file-2.tsv')
], Tinderbox\Clickhouse\Common\Format::TSV);
```

In case of `writeFiles` queries executes asynchronously. If you have butch of files and you want to insert them in one insert query, you can
use our `ccat` utility and `MergedFiles` instance instead of `File`. You should put list of files to insert into
one file:

```
file-1.tsv
file-2.tsv
```

### Building ccat

`ccat` sources placed into `utils/ccat` directory. Just run `make && make install` to build and install library into
`bin` directory of package. There are already compiled binary of `ccat` in `bin` directory, but it
may not work on some systems.

**In `writeFiles` method, you can pass the parameter `$concurrency` which is responsible for the maximum simultaneous number of requests.**

## Other queries

In addition to SELECT and INSERT queries, you can execute other queries :) There is `statement` method for this purposes.

```php
$client->writeOne('DROP TABLE table');
```

## Testing

``` bash
$ composer test
```

## Roadmap

* Add ability to save query result in local file

## Contributing
Please send your own pull-requests and make suggestions on how to improve anything.
We will be very grateful.

Thx!
