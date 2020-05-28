<?php
namespace Swango\MQ\TaskQueue;
class Sender {
    public $in_pool = true, $is_available = true, $channel;
    public function __construct() {
        $this->channel = Connection::getChannel();
    }
    public function __destruct() {
        try {
            $this->channel->close();
            unset($this->channel);
        } catch (\Throwable $e) {
        };
        SenderPool::subCounter();
    }
}