<?php
namespace Swango\MQ\TaskQueue;
use Bunny\Message;
use Swango\Environment;
use Swango\MQ\TaskQueue\Handler\AbstractController;
/**
 * Class task
 * @property string $uuid 唯一ID
 * @property int $create_time  最初task创建时间
 * @property int $exec_time  执行时间如果输入小于当前时间则会变成  time() + $exec_time  为空则立即执行
 * @property string $handler  对应daemon里的Process
 * @property string $params  作为handler的参数
 * @property int $attempt 已经尝试次数
 * @property int $max_attempt 最大尝试次数 为null则不限制次数
 * @property int $queue_type [timing 延时队列] [pending 待执行队列] [log_recycle 失败队列 待写入日志后移除]
 */
class Task {
    const HANDLER_TYPE_CONTROLLER = 'controller';
    const QUEUE_TYPE_TIMING = 'timing', QUEUE_TYPE_PENDING = 'pending', QUEUE_TYPE_LOG_RECYCLE = 'log_recycle';
    private $uuid, $exec_time, $create_time, $handler, $params, $queue_type, $attempt, $max_attempt;
    private $receive_msg, $error;
    private function __construct() {
    }
    /**
     * @param $handler
     * @param $params
     * @param int|null $exec_time 为null则马上执行
     * @param int|null $max_attempt
     * @return Task
     */
    public static function init($handler, $params, ?int $exec_time = null, ?int $max_attempt = null): self {
        $params = base64_encode(\Swoole\Serialize::pack($params));
        if (isset($exec_time)) {
            if ($exec_time < time()) {
                $exec_time += time();
            }
            $queue_type = self::QUEUE_TYPE_TIMING;
        } else {
            $queue_type = self::QUEUE_TYPE_PENDING;
        }
        $uuid = \XString\GenerateRandomString(18) . sprintf('%014d', microtime(true) * 1000);
        $obj = new self();
        $obj->uuid = $uuid;
        $obj->handler = $handler;
        $obj->params = $params;
        $obj->attempt = 0;
        $obj->max_attempt = $max_attempt;
        $obj->create_time = time();
        $obj->exec_time = $exec_time;
        $obj->queue_type = $queue_type;
        return $obj;
    }
    public function getQueueType(): string {
        return $this->queue_type;
    }
    public static function initByAMQPMessage(Message $msg) {
        $body = $msg->content;
        $error = null;
        if ($msg->exchange === self::QUEUE_TYPE_LOG_RECYCLE) {
            [
                'uuid' => $uuid,
                'handler' => $handler,
                'params' => $params,
                'attempt' => $attempt,
                'max_attempt' => $max_attempt,
                'create_time' => $create_time,
                'error' => $error
            ] = \Json::decodeAsArray($body);
        } else {
            [
                'uuid' => $uuid,
                'handler' => $handler,
                'params' => $params,
                'attempt' => $attempt,
                'max_attempt' => $max_attempt,
                'create_time' => $create_time,
            ] = \Json::decodeAsArray($body);
        }
        $obj = new self();
        $obj->uuid = $uuid;
        $obj->receive_msg = $msg;
        $obj->handler = $handler;
        $obj->params = $params;
        $obj->create_time = $create_time;
        $obj->attempt = $attempt;
        $obj->max_attempt = $max_attempt;
        $obj->queue_type = $msg->exchange;
        $obj->error = $error;
        return $obj;
    }
    public function recycle(\Throwable $e) {
        if ($this->queue_type !== self::QUEUE_TYPE_PENDING) {
            throw new Exception\RuntimeException('Invalid task to recycle');
        }
        $this->queue_type = self::QUEUE_TYPE_LOG_RECYCLE;
        $this->error = $e->getMessage() . '|' . $e->getTraceAsString();
        Sender::send($this);
        return true;
    }
    public function getMessageBody(): string {
        $body = [
            'uuid' => $this->uuid,
            'handler' => $this->handler,
            'params' => $this->params,
            'attempt' => $this->attempt,
            'max_attempt' => $this->max_attempt,
            'create_time' => $this->create_time,
            'exec_time' => $this->exec_time
        ];
        if ($this->queue_type === self::QUEUE_TYPE_LOG_RECYCLE) {
            $body['error'] = $this->error;
        }
        return \Json::encode($body);
    }
    public function getMessageHeaders(): array {
        $properties = [];
        $properties['delivery-mode'] = 2;  // 持续化
        if ($this->queue_type === self::QUEUE_TYPE_TIMING) {
            $expiration = ($this->exec_time - time()) * 1000;
            $expiration = $expiration > 0 ? $expiration : 0;
            $properties['expiration'] = $expiration;
        }
        return $properties;
    }
    public function execTask(): bool {
        if (! isset($this->receive_msg)) {
            throw new Exception\RuntimeException('error task execute');
        }
        $this->attempt = $this->attempt + 1;
        switch ($this->queue_type) {
            case self::QUEUE_TYPE_LOG_RECYCLE:
                return $this->recycleTaskHandle();
                break;
            case self::QUEUE_TYPE_PENDING:
                return $this->pendingTaskHandle();
                break;
            default:
                throw new Exception\RuntimeException('error task queue type');
        }
    }
    private function getHandlerController(): AbstractController {
        [
            'handler_namespace' => $handler_namespace
        ] = Environment::getConfig('mq_task');
        $class_name = $handler_namespace . str_replace('_', '\\', $this->handler) . '\\Controller';
        if (! class_exists($class_name)) {
            throw new Exception\RuntimeException(sprintf('handler class [%s] is not exists', $class_name));
        }
        if (! is_callable([
            $class_name,
            'handle'
        ])) {
            throw new Exception\RuntimeException(sprintf('handler [%s::handle] is not callable', $class_name));
        }
        return $handler = new $class_name($this);
    }
    private function getParams() {
        return \Swoole\Serialize::unpack(base64_decode($this->params));
    }
    private function pendingTaskHandle() {
        try {
            $controller = $this->getHandlerController();
            $result = $controller->handle($this->getParams(), $retry_time);
            if (! isset($result)) {
                return $this->retry($retry_time ?? 60);
            } else {
                return $result;
            }
        } catch (\Throwable $e) {
            return $this->recycle($e);
        }
    }
    private function retry(int $exec_time = null) {
        if (isset($this->max_attempt) && $this->attempt >= $this->max_attempt) {
            $queue_type = self::QUEUE_TYPE_LOG_RECYCLE;
        } elseif (isset($exec_time)) {
            if ($exec_time < time()) {
                $exec_time += time();
            }
            $queue_type = self::QUEUE_TYPE_TIMING;
        } else {
            $queue_type = self::QUEUE_TYPE_PENDING;
        }
        $this->exec_time = $exec_time;
        $this->queue_type = $queue_type;
        Sender::send($this);
        return true;
    }
    private function recycleTaskHandle() {
        $dir = \Swango\Environment::getDir()->log . 'task_error/';
        if (! is_dir($dir)) {
            mkdir($dir);
        }
        $s = sprintf('[%s] : ', date('Y-m-d H:i:s', time()));
        $s .= sprintf('%s,', $this->getMessageBody());
        $fp = fopen($dir . date('Y-m-d') . '.log', 'a');
        fwrite($fp, $s);
        fclose($fp);
        return true;
    }
}