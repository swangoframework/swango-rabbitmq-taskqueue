<?php
namespace Swango\MQ\TaskQueue;
class Sender {
    /**
     * @var SenderPool
     */
    private static $pool;
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
        if (! isset(static::$pool)) {
            static::$pool = new SenderPool();
        }
        try {
            $sender = static::$pool->pop();
            $sender->channel->publish($task->getMessageBody(), $task->getMessageHeaders(), $task->getQueueType(),
                $task->getQueueType());
            static::$pool->push($sender);
        } catch (\Throwable $e) {
            $sender->is_available = false;
            static::$pool->push($sender);
            return self::send($task);
        }
    }
}