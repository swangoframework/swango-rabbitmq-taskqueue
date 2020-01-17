<?php
namespace Swango\MQ\TaskQueue;
class Sender {
    private static $channel_pool;
    public static function send(Task $task) {
        $channel = Connection::getChannel();
        switch ($task->getQueueType()) {
            case Task::QUEUE_TYPE_TIMING:
                $channel->exchangeDeclare(Task::QUEUE_TYPE_TIMING);
                $channel->exchangeDeclare(Task::QUEUE_TYPE_PENDING);
                $channel->queueDeclare(Task::QUEUE_TYPE_TIMING, false, true, false, false, false, [
                    'x-dead-letter-exchange' => Task::QUEUE_TYPE_PENDING,
                    'x-dead-letter-routing-key' => Task::QUEUE_TYPE_PENDING
                ]);
                $channel->queueBind(Task::QUEUE_TYPE_TIMING, Task::QUEUE_TYPE_TIMING, Task::QUEUE_TYPE_TIMING);
                $channel->queueDeclare(Task::QUEUE_TYPE_PENDING, false, true, false, false, false);
                $channel->queueBind(Task::QUEUE_TYPE_PENDING, Task::QUEUE_TYPE_PENDING, Task::QUEUE_TYPE_PENDING);
                break;
            case Task::QUEUE_TYPE_PENDING:
                $channel->exchangeDeclare(Task::QUEUE_TYPE_PENDING);
                $channel->queueDeclare(Task::QUEUE_TYPE_PENDING, false, true, false, false, false);
                $channel->queueBind(Task::QUEUE_TYPE_PENDING, Task::QUEUE_TYPE_PENDING, Task::QUEUE_TYPE_PENDING);
                break;
            case Task::QUEUE_TYPE_LOG_RECYCLE:
                $channel->exchangeDeclare(Task::QUEUE_TYPE_LOG_RECYCLE);
                $channel->queueDeclare(Task::QUEUE_TYPE_LOG_RECYCLE, false, true, false, false, false);
                $channel->queueBind(Task::QUEUE_TYPE_LOG_RECYCLE, Task::QUEUE_TYPE_LOG_RECYCLE,
                    Task::QUEUE_TYPE_LOG_RECYCLE);
                break;
            default:
                throw new \RuntimeException('known error');
        }
        try {
            $channel->publish($task->getMessageBody(), $task->getMessageHeaders(), $task->getQueueType(),
                $task->getQueueType());
        } catch (\Throwable $e) {
            unset($channel);
            return self::send($task);
        }
        unset($channel);
    }
}