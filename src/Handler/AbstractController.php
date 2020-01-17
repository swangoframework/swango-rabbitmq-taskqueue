<?php
namespace Swango\MQ\TaskQueue\Handler;
abstract class AbstractController {
    /**
     * @var \Swango\MQ\TaskQueue\Task $task
     */
    protected $task;
    public function __construct(\Swango\MQ\TaskQueue\Task $task) {
        $this->task = $task;
    }
    abstract function handle($params, &$retry_time): ?bool;
}