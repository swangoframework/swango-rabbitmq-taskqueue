<?php
namespace Swango\MQ\TaskQueue;
class SenderPool {
    protected static $atomic, $too_many_connection_lock, $max_connection, $count = 0;
    private static $pool;
    public static function init(): void {
        static::$atomic = new \Swoole\Atomic();
        static::$too_many_connection_lock = new \Swoole\Atomic();
        static::$max_connection = \Swango\Environment::getConfig('rabbitmq')['sender_num'];
    }
    public static function subCounter(): int {
        --static::$count;
        if (isset(static::$atomic)) {
            return static::$atomic->sub(1);
        }
        return 0;
    }
    public static function getWorkerCount(): int {
        return static::$count;
    }
    public static function addWorkerCountToAtomic(bool $set_to_zero_first = false): void {
        if ($set_to_zero_first) {
            static::$atomic->set(static::$count);
        } else {
            static::$atomic->add(static::$count);
        }
    }
    protected $server_info, $queue, $channel, $timer;
    private const TIMEOUT = 25;
    private function __construct() {
        $this->channel = new \Swoole\Coroutine\Channel(1);
        $this->queue = new \SplQueue();
        \swoole_timer_after(rand(50, 5000), '\\swoole_timer_tick', 10000, [
            $this,
            'checkPool'
        ]);
    }
    protected function newSender(): ?Sender {
        $use_max_limit = isset(static::$too_many_connection_lock) && isset(static::$atomic) &&
            isset(static::$max_connection);
        if ($use_max_limit) {
            if (static::$too_many_connection_lock->get() > time()) {
                trigger_error("SenderPool: new channel fail because of lock");
                return null;
            }
            if (static::$count >= static::$max_connection) {
                trigger_error('SenderPool: new channel fail because reach max connections for each worker:' .
                    static::$count);
                return null;
            }
            // 新增连接时，若已达上限，则返回空
            $count = static::$atomic->add(1);
            if ($count > static::$max_connection) {
                static::$atomic->sub(1);
                trigger_error("SenderPool: new channel fail because reach max connections: $count");
                return null;
            }
        }
        try {
            ++static::$count;
            $sender = new Sender();
            return $sender;
        } catch (\Throwable $e) {
            if ($use_max_limit) {
                // 10秒内不再尝试新建连接
                static::$too_many_connection_lock->set(\Time\now() + 10);
                trigger_error("SenderPool: new channel fail because get rabbitMQ connection fail");
                return null;
            }
            throw $e;
        }
    }
    public function push(Sender $sender): void {
        if ($sender->in_pool) {
            trigger_error("SenderPool: Already in pool ");
            return;
        }
        // 因为各种原因，push失败了，要抛弃该条连接，总连接数减1
        if (! $sender->is_available) {
            $sender->channel->close();
            unset($sender);
            trigger_error("SenderPool: push fail because not connected");
            return;
        }
        $sender->in_pool = true;
        if ($this->channel->stats()['consumer_num'] > 0) {
            $this->channel->push($sender);
        } else {
            $this->queue->push($sender);
        }
    }
    public function pop(): Sender {
        // 如果通道为空，则试图创建，若已达到最大连接数，则注册消费者，等待新的连接
        do {
            if ($this->queue->isEmpty()) {
                $sender = $this->newSender();
                if (! isset($sender)) {
                    $sender = $this->channel->pop(self::TIMEOUT);
                    if ($sender === false) {
                        throw new Exception\RuntimeException('Channel pop timeout');
                    }
                }
                $sender->in_pool = false;
                return $sender;
            }
            $sender = $this->queue->pop();
        } while (! $sender->is_available);
        $sender->in_pool = false;
        return $sender;
    }
    public function checkPool(?int $timer_id = null) {
        if (isset($timer_id)) {
            if (isset($this->timer) && $this->timer !== $timer_id) {
                \swoole_timer_clear($this->timer);
            }
            $this->timer = $timer_id;
        }
        $count = $this->queue->count();
        if ($count === 0) {
            if ($this->channel->stats()['consumer_num'] > 0) {
                trigger_error('New db because there are more than one consumer. This is abnormal.');
                $sender = $this->newSender();
                if (isset($sender)) {
                    $this->push($sender);
                }
            }
            return;
        }
        // $average_dbs_for_each_worker = intdiv(static::$max_conntection, \Server::getWorkerNum());
        // $max_spare_dbs = intdiv($average_dbs_for_each_worker, 8);
        if ($count > 1) {
            // trigger_error(LOCAL_IP . ' ' . \Server::getWorkerId() . " pop db because there are too many connections");
            $sender = $this->queue->pop();
            unset($sender);
        }
    }
    public function clearQueueAndTimer(): void {
        if (isset($this->timer)) {
            \swoole_timer_clear($this->timer);
        }
        $this->queue = new \SplQueue();
    }
    public static function getPool(): self {
        if (! isset(static::$pool)) {
            static::$pool = new self();
        }
        return static::$pool;
    }
}