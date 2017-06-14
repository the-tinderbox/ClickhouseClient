# Clickhouse Client
[![Build Status](https://travis-ci.org/the-tinderbox/ClickhouseClient.svg?branch=master)](https://travis-ci.org/the-tinderbox/ClickhouseClient) [![Coverage Status](https://coveralls.io/repos/github/the-tinderbox/ClickhouseClient/badge.svg?branch=master)](https://coveralls.io/github/the-tinderbox/ClickhouseClient?branch=master)

Package was written as client for [Clickhouse](https://clickhouse.yandex/).

Client uses [Guzzle](https://github.com/guzzle/guzzle) for sending Http requests to Clickhouse servers.
 
## Requirements
`php7.1`

## Install

Composer

```bash
composer require the-tinderbox/clickhouse-php-client
```

# Using

Client works with alone server and cluster. Also, client can make async select and insert (from local files) queries.

## Alone server

```php
$server = new Tinderbox\Clickhouse\Server('127.0.0.1', '8123', 'default', 'user', 'pass');
$client = new Tinderbox\Clickhouse\Client($server);
```

## Cluster

```php
$cluster = new Tinderbox\Clickhouse\Cluster([
    'server-1' => [
        'host' => '127.0.0.1',
        'port' => '8123',
        'database' => 'default',
        'user' => 'user',
        'password' => 'pass'
    ],
    'server-2' => new Tinderbox\Clickhouse\Server('127.0.0.1', '8124', 'default', 'user', 'pass')
]);
$client = new Tinderbox\Clickhouse\Client($cluster);
```

By default client will use first server in given list. If you want to perform request on another server you should use 
`using($hostname)` method on client and then run query;

```php
$client->using('server-2')->select('select * from table');
```

## Values mappers

There are two ways to bind values into raw sql:

1. Unnamed placeholders like `?` in query;
2. Named placeholders like `:paramname` in query.

To choose the right one, you can pass mapper to client constructor as second argument.

```php
$unnamedMapper = new Tinderbox\Clickhouse\Query\Mapper\UnnamedMapper();
$namedMapper = new Tinderbox\Clickhouse\Query\Mapper\NamedMapper();

$client = new Tinderbox\Clickhouse\Client($server, $unnamedMapper);
$client = new Tinderbox\Clickhouse\Client($server, $namedMapper);
```

By default client uses `UnnamedMapper`.

### Unnamed

In case of unnamed placeholder, bindings array should be non-associative.

```php
$client->select('select * from table where column = ?', [1]);
```

### Named

In case of named placeholder, bindings array should be associative where each key corresponds to placeholder in query.

Key may contain `:` or not, but placeholder in query must have `:`.

```php
$client->select('select * from table where column = :column', [
    'column' => 1
]);
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
```

### Sync

```php
$result = $client->select('select number from system.numbers limit 100');

foreach ($result as $number) {
    echo $number['number'].PHP_EOL;
}
```

**Using local files**

You can use local files as temporary tables in Clickhouse. You should pass as second argument array of `TempTable` instances or single `TempTable`
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
$result = $client->select('select number from system.numbers where number in _numbers limit 100', new TempTable('_numbers', 'numbers.csv', [
    'number' => 'UInt64'
]));

foreach ($result as $number) {
    echo $number['number'].PHP_EOL;
}
```

### Async

Unlike the `select` method, which returns` Result`, the `selectAsync` method returns an array of` Result` for each executed query.

```php

list($clicks, $visits, $views) = $client->selectAsync([
    ['select * from clicks where date = ?', ['2017-01-01']],
    ['select * from visits where date = ?', ['2017-01-01']],
    ['select * from views where date = ?', ['2017-01-01']],
]);

foreach ($clicks as $click) {
    echo $click['date'].PHP_EOL;
}

```
**In `selectAsync` method, you can pass the parameter `$concurrency` which is responsible for the maximum simultaneous number of requests.**

**Using local files**

As with synchronous select request you can pass files to the server:

```php

list($clicks, $visits, $views) = $client->selectAsync([
    ['select * from clicks where date = ? and userId in _users', ['2017-01-01'], new TempTable('_users', 'users.csv', ['number' => 'UInt64'])],
    ['select * from visits where date = ?', ['2017-01-01']],
    ['select * from views where date = ?', ['2017-01-01']],
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
$client->insert('insert into table (date, column) values (?,?), (?,?)', ['2017-01-01', 1, '2017-01-02', 2]);

$client->insertFiles('table', ['date', 'column'], [
    '/file-1.csv',
    '/file-2.csv'
]);

$client->insertFiles('table', ['date', 'column'], [
    '/file-1.tsv',
    '/file-2.tsv'
], Tinderbox\Clickhouse\Common\Format::TSV);
```

In case of `insertFiles` queries exetues asynchronously

**In `insertFiles` method, you can pass the parameter `$concurrency` which is responsible for the maximum simultaneous number of requests.**

## Other queries

In addition to SELECT and INSERT queries, you can execute other queries :) There is `statement` method for this purposes.

```php
$client->statement('DROP TABLE table');
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
