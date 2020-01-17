<?php
namespace Swango\MQ\TaskQueue;
use Bunny\Channel;
use Bunny\Client;
use Bunny\Message;
use Swango\Environment;
class Receiver {
    static $count = 0;
    public static function receive($queue_type, Channel $channel) {
        $channel->run(function (Message $message, Channel $channel, Client $bunny) {
            $task = Task::initByAMQPMessage($message); // Handle your message here
            $result = $task->execTask();
            if ($result) {
                $channel->ack($message); // Acknowledge message
            } else {
                $channel->nack($message); // Mark message fail, message will be redelivered
            }
            self::$count++;
            if (self::$count % 1000 === 0) {
                self::$count = 0;
                gc_collect_cycles();
                echo memory_get_usage() / 1024, "k\n";
            }
        }, $queue_type);
    }
    public static function run() {  // Swoole\Runtime::enableCoroutine(true);   Swoole\Runtime::enableCoroutine(true);
        self::initQueue();
        [
            'receiver_num' => $receiver_num,
            'recycle_receiver_num' => $recycle_receiver_num
        ] = Environment::getConfig('mq_task');
        $receive_pool = new \Swoole\Coroutine\Channel($receiver_num);
        for ($i = 0; $i < $receiver_num; $i++) {
            $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
            $receive_pool->push([
                \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING,
                $channel
            ]);
        }
        for ($i = 0; $i < $recycle_receiver_num; $i++) {
            $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
            $receive_pool->push([
                \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_LOG_RECYCLE,
                $channel
            ]);
        }
        /**
         * @var \Bunny\Client $channel
         */
        while ($data = $receive_pool->pop()) {
            go(function () use ($data, $receive_pool) {
                list($queue_type, $channel) = $data;
                try {
                    \Swango\MQ\TaskQueue\Receiver::receive($queue_type, $channel);
                } catch (\Throwable $e) {
                    trigger_error(get_class($e) . ' ' . $e->getMessage() . ' :' . $e->getTraceAsString());
                    if (isset($channel)) {
                        unset($channel);
                        $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
                    }
                }
                $receive_pool->push([
                    $queue_type,
                    $channel
                ]);
            });
        }
    }
    private static function initQueue() {
        $channel = \Swango\MQ\TaskQueue\Connection::getChannel();
        $channel->exchangeDeclare(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING);
        $channel->queueDeclare(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING, false, true, false, false, false);
        $channel->queueBind(\Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING,
            \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING, \Swango\MQ\TaskQueue\Task::QUEUE_TYPE_PENDING);
        $channel->exchangeDeclare(Task::QUEUE_TYPE_LOG_RECYCLE);
        $channel->queueDeclare(Task::QUEUE_TYPE_LOG_RECYCLE, false, true, false, false, false);
        $channel->queueBind(Task::QUEUE_TYPE_LOG_RECYCLE, Task::QUEUE_TYPE_LOG_RECYCLE, Task::QUEUE_TYPE_LOG_RECYCLE);
        $channel->close();
    }
}