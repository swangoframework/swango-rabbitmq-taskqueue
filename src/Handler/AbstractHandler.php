<?php
namespace Swango\MQ\TaskQueue\Handler;
abstract class AbstractHandler {
    /**
     * @var \Swango\MQ\TaskQueue\Task $task
     */
    protected $task;
    public function __construct(\Swango\MQ\TaskQueue\Task $task) {
        $this->task = $task;
    }
    /*
     * return
     *          [true 确认 ack message]
     *          [false 取消 neck message]
     *          [null 重试 确认当前message 重新生成新task 插入rabbit MQ Timing 队列]
     */
    abstract function handle($params, &$retry_time): ?bool;
}