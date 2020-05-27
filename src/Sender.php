<?php
namespace Swango\MQ\TaskQueue;
class Sender {
    public $in_pool = true, $is_available = true, $channel;
    public function __construct() {
        $this->channel = Connection::getChannel();
    }
    public function __destruct() {
        $this->channel->close();
        unset($this->channel);
        SenderPool::subCounter();
    }
    public static function send(Task $task) {
        $pool = SenderPool::getPool();
        try {
            $sender = $pool->pop();
            $sender->channel->publish($task->getMessageBody(), $task->getMessageHeaders(), $task->getQueueType(),
                $task->getQueueType());
            $pool->push($sender);
        } catch (\Throwable $e) {
            $sender->is_available = false;
            $pool->push($sender);
            return self::send($task);
        }
    }
}