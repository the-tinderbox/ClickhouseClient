<?php

namespace Tinderbox\Clickhouse\Exceptions;

class ClientException extends \Exception
{
    public static function invalidServerProvided($server)
    {
        return new static(
            'Invalid server provided. Server must be the type of Server or Cluster, but '.gettype(
                $server
            ).' given'
        );
    }

    public static function clusterIsNotProvided()
    {
        return new static('Can not execute query using specified server because cluster is not provided');
    }

    public static function connectionError()
    {
        return new static('Can\'t connect to the server');
    }

    public static function serverReturnedError($error)
    {
        return new static('Server returned error: '.$error);
    }

    public static function malformedResponseFromServer($response)
    {
        return new static('Malformed response from server: '.$response);
    }

    public static function insertFileNotFound($file)
    {
        return new static('File '.$file.' is not found');
    }
}
