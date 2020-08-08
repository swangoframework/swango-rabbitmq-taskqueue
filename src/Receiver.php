<?php
namespace Swango\MQ\TaskQueue;
use Bunny\Channel;
use Bunny\Client;
use Bunny\Message;
use Swango\Environment;
class Receiver {
    private static $pool;
    public static function receive($queue_type, Channel $channel, \Swoole\Server $server) {
        $channel->run(function (Message $message, Channel $channel, Client $bunny) use ($server) {
            $data_str = Task::getTaskJsonByMessage($message);
            $result = $server->taskwait(pack('CC', 9, 1) . $data_str, 60);
            if (false === $result) {
                $channel->nack($message); // Mark message fail, message will be redelivered
            } else {
                $channel->ack($message); // Acknowledge message
            }
        }, $queue_type);
    }
    public static function taskHandle(string $data_str) {
        $result_channel = new \Swoole\Coroutine\Channel();
        go(function () use ($result_channel, $data_str) {
            $task = Task::initByAMQPMessageJson($data_str); // Handle your message here\
            $result = $task->execTask();
            $result_channel->push($result);
        });
        return $result_channel->pop();
    }
    public static function stop() {
        if (isset(self::$pool)) {
            foreach (self::$pool as $i => $channel) {
                try {
                    $channel->close();
                } catch (\Throwable $e) {
                }
                unset($channel);
            }
            self::$pool = null;
        }
    }
    public static function run(\Swoole\Server $server) {  // run on  Swoole\Runtime::enableCoroutine(true);
        go(function () use ($server) {
            self::initQueue();
            if (! isset(self::$pool)) {
                self::$pool = [];
            }
            [
                'receiver_num' => $receiver_num,
                'recycle_receiver_num' => $recycle_receiver_num
            ] = Environment::getConfig('rabbitmq');
            $receive_pool = new \Swoole\Coroutine\Channel($receiver_num + $recycle_receiver_num);
            for ($i = 0; $i < $receiver_num; $i++) {
                $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
                $receive_pool->push([
                    \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING,
                    $channel,
                    $i
                ]);
            }
            for ($i = 0; $i < $recycle_receiver_num; $i++) {
                $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
                $receive_pool->push([
                    \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_LOG_RECYCLE,
                    $channel,
                    $i + $receiver_num
                ]);
            }
            /**
             * @var \Bunny\Client $channel
             */
            while ($data = $receive_pool->pop()) {
                go(function () use ($data, $receive_pool, $server) {
                    list($queue_type, $channel, $i) = $data;
                    try {
                        self::$pool[$i] = $channel;
                        \Swango\MQ\TaskQueue\Receiver::receive($queue_type, $channel, $server);
                    } catch (\Throwable $e) {
                        trigger_error(get_class($e) . ' ' . $e->getMessage() . ' :' . $e->getTraceAsString());
                        if (isset($channel)) {
                            unset(self::$pool[$i]);
                            unset($channel);
                            $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
                        }
                    }
                    $receive_pool->push([
                        $queue_type,
                        $channel,
                        $i
                    ]);
                });
            }
        });
    }
    private static function initQueue() {
        $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
        $channel->exchangeDeclare(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_TIMING);
        $channel->queueDeclare(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_TIMING, false, true, false, false, false, [
            'x-dead-letter-exchange' => \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING,
            'x-dead-letter-routing-key' => \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING
        ]);
        $channel->queueBind(Task::QUEUE_TYPE_TIMING, Task::QUEUE_TYPE_TIMING, Task::QUEUE_TYPE_TIMING);
        $channel->exchangeDeclare(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING);
        $channel->queueDeclare(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING, false, true, false, false, false);
        $channel->queueBind(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING,
            \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING, \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING);
        $channel->exchangeDeclare(Task::QUEUE_TYPE_LOG_RECYCLE);
        $channel->queueDeclare(Task::QUEUE_TYPE_LOG_RECYCLE, false, true, false, false, false);
        $channel->queueBind(Task::QUEUE_TYPE_LOG_RECYCLE, Task::QUEUE_TYPE_LOG_RECYCLE, Task::QUEUE_TYPE_LOG_RECYCLE);
        $channel->close();
        unset($channel);
    }
}