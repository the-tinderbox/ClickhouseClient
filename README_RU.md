# Clickhouse Client
[![Build Status](https://travis-ci.org/the-tinderbox/ClickhouseClient.svg?branch=master)](https://travis-ci.org/the-tinderbox/ClickhouseClient) [![Coverage Status](https://coveralls.io/repos/github/the-tinderbox/ClickhouseClient/badge.svg?branch=master)](https://coveralls.io/github/the-tinderbox/ClickhouseClient?branch=master)

Пакет разработан для работы с [Clickhouse](https://clickhouse.yandex/).

Клиент использует [Guzzle](https://github.com/guzzle/guzzle) для отправки Http запросов на сервера Clickhouse.
 
## Requirements
`php7.1`

## Install

Composer

```bash
composer require the-tinderbox/clickhouse-php-client
```

# Using

Клиент умеет работать как с одним сервером так и с кластером. Так же клиент умеет выполнять асинхронные
select запросы и асинхронные запросы на запись из локальных файлов.

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

По умолчанию клиент будет использовать первый в списке сервер для выполнения запросов. Для того, что бы
выполнить запрос на другом сервере необходимо вызвать метод `using($hostname)` на клиенте и затем выполнять
запрос.

```php
$client->using('server-2')->select('select * from table');
```

## Values mappers

Есть два способа передавать значения в запрос:

1. Неименованные плейсхолдеры в виде `?` в запросе;
2. Именованные плейсхолдеры в виде `:paramname` в запросе.

Что бы выбрать нужный биндер которым будет удобнее пользоваться, можно вторым аргументов в конструктор клиента
передать нужный биндер.

```php
$unnamedMapper = new Tinderbox\Clickhouse\Query\Mapper\UnnamedMapper();
$namedMapper = new Tinderbox\Clickhouse\Query\Mapper\NamedMapper();

$client = new Tinderbox\Clickhouse\Client($server, $unnamedMapper);
$client = new Tinderbox\Clickhouse\Client($server, $namedMapper);
```

По умолчанию используется `UnnamedMapper`.

### Unnamed

В случае с неименованными плейсхолдерами, массив значений должен быть не ассоциативный.

```php
$client->select('select * from table where column = ?', [1]);
```

### Named

В случае с именованными плейсхолдерами, массив значений должен быть ассоциативным где каждый ключ
соответствует плейсхолдеру в запросе.

Ключ может содержать `:`, так же может использоваться без `:`, но в запросе перед плейсхолдером `:` всегда
должно быть.

```php
$client->select('select * from table where column = :column', [
    'column' => 1
]);
```

## Select queries

Любой SELECT запрос вернет результат в виде `Result`. Этот класс реализует интерфейсы `\ArrayAccess`, `\Countable` и `\Iterator`,
что означает, что с ним можно обращаться как с обычным массивом.

Чистый массив значений можно получить обратившись к свойству `rows`

```php
$rows = $result->rows;
$rows = $result->getRows();
```

Так же результат содержит статистические данные выполнения запроса:

1. Количество прочитанных строк
2. Количество прочитанных байт
3. Время выполнения запроса

Что бы получить статистику нужно обратиться к свойству `statistic`

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

**Использование локальных файлов**

В запросе можно использовать данные из локальных файлов. Для этого необходимо вторым аргументом передать либо массив
из набора `TempTable` либо одиночный `TempTable`.

В этом случае будет отправлен один файл на сервер данные из которого будут загруженны во временную таблицу `_numbers`.
Структура таблицы будет такой:

* number - UInt64

Если же передать в качестве структуры такой массив:

```php
['UInt64']
```

То каждая колонка из файла будет просто пронумерована как _1, _2, _3.

```php
$result = $client->select('select number from system.numbers where number in _numbers limit 100', new TempTable('_numbers', 'numbers.csv', [
    'number' => 'UInt64'
]));

foreach ($result as $number) {
    echo $number['number'].PHP_EOL;
}
```

### Async

В отличии от `select` метода, который возвращает `Result` метод `selectAsync` вернет массив из `Result` для 
каждого выполненного запроса.

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
**В метод `selectAsync` можно передать параметр `$concurrency` который отвечает за максимальное одновременное количество запросов.**

**Использование локальных файлов**

Как и с синхронным select запросом можно передавать файлы на сервер. Делается это так:

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

С асинхронными запросами можно передавать больше одного файла, так же как и с синхронным запросом.

## Insert queries

Запросы на запись данных всегда возвращают true или выбрасывают исключение в случае ошибки.

Данные можно записать построчно или же асинхронно из набора CSV или TSV файлов.

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

В случае с `insertFiles` запросы выполняются асинхронно.

**В метод `insertFiles` можно передать параметр `$concurrency` который отвечает за максимальное одновременное количество запросов.**

## Other queries

Помимо SELECT и INSERT запросов можно выполнять другие запросы :) Для этого существует метод `statement`.

```php
$client->statement('DROP TABLE table');
```

## Testing

``` bash
$ composer test
```

## Roadmap

* Cделать возможность сохранять результат запроса в локальный файл

## Contributing
Пожалуйста, присылайте свои пул-реквесты и высказывайте предложения по улучшению чего-либо.
Будем очень благодарны.

Спасибо!
