<?php
namespace Swango\MQ\TaskQueue;
use Bunny\Channel;
use Bunny\Client;
use Swango\Environment;
class Connection {
    private static function getConnection(): Client {
        $config = Environment::getConfig('rabbit_mq');
        $connection = [
            'host' => $config['host'],
            'vhost' => $config['vhost'],
            'user' => $config['username'],
            'password' => $config['password'],
        ];
        $client = new Client($connection);
        $client->connect();
        return $client;
    }
    public static function getChannel(): Channel {
        return self::getConnection()->channel();
    }
}