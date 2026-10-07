"""工作窃取调度器内核。

每个执行者（worker）拥有一台本地双端队列。调度完全由调用方以整数步驱动：
每调用一次 step() 恰好消耗一个时间片，每一步只有一个执行者拿到时间片。
不涉及真实线程、真实时钟和 I/O，同一份输入下的行为完全确定。

本地队列约定：

- 归属：注册任务时用 worker 参数指定它的归属执行者；任务第一次进入队列时
  放在归属执行者的本地队列尾部，依赖全部就绪后同样回到归属执行者的队列；
- 本地取出：执行者从自己队列的队尾取任务（后进先出）；一个任务没跑完就回到
  当前执行者的队尾，剩余工作跟着当前执行者走；
- 窃取：本地队列为空的执行者，从相邻编号开始绕圈找第一个队列非空的执行者，
  从它的队首搬走一半（向上取整）个任务放到自己队尾，并立刻执行一个单位；
- 就绪：只有依赖全部完成的任务才会进入就绪队列；
- 空转：只有任何执行者都没有可运行任务的那一步才算空闲时间片。

统计口径：busy 是执行了工作单位的时间片数，idle 是空转的时间片数，
任何时刻已消耗的时间片数 ticks 都等于 busy + idle。
"""

from collections import deque

DEFAULT_WORKERS = 2
DEFAULT_MAX_STEPS = 100000

WAITING = "waiting"
PENDING = "pending"
READY = "ready"
RUNNING = "running"
DONE = "done"

RUN = "run"
STEAL = "steal"
IDLE = "idle"


class Worker:
    """一个执行者，以及它的本地双端队列和负载计数。"""

    __slots__ = ("wid", "deque", "executed", "steals", "donated")

    def __init__(self, wid):
        self.wid = wid
        self.deque = deque()
        self.executed = 0
        self.steals = 0
        self.donated = 0

    def __repr__(self):
        return "Worker(%d)" % self.wid


class Task:
    """一个待执行的工作任务。"""

    __slots__ = ("name", "units", "remaining", "deps", "worker", "state",
                 "gated", "created_at", "finished_at", "executions")

    def __init__(self, name, units, worker):
        self.name = name
        self.units = units
        self.remaining = units
        self.deps = ()
        self.worker = worker
        self.state = READY
        self.gated = False
        self.created_at = 0
        self.finished_at = None
        self.executions = 0

    def __repr__(self):
        return "Task(%s)" % self.name


class WorkStealingScheduler:
    """确定性的工作窃取调度器。"""

    def __init__(self, workers=DEFAULT_WORKERS):
        if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
            raise ValueError("执行者数量必须是正整数")
        self.workers = [Worker(wid) for wid in range(workers)]
        self.tasks = {}
        self.ticks = 0
        self.busy_ticks = 0
        self.idle_ticks = 0
        self.steal_ops = 0
        self.stolen_tasks = 0
        self.history = []
        self._cursor = 0

    # ------------------------------------------------------------ 对外接口

    def add(self, name, units=1, deps=(), worker=0, gated=False):
        """注册任务；依赖未全部完成或 gate 未打开时先不进本地队列。"""
        if not isinstance(name, str) or not name:
            raise ValueError("任务名必须是非空字符串")
        if name in self.tasks:
            raise ValueError("任务名重复: %s" % name)
        if isinstance(units, bool) or not isinstance(units, int) or units < 1:
            raise ValueError("工作单位数必须是正整数")
        if isinstance(worker, bool) or not isinstance(worker, int) \
                or not 0 <= worker < len(self.workers):
            raise ValueError("执行者编号越界: %r" % (worker,))
        if isinstance(deps, str):
            raise ValueError("依赖必须是一串任务名")
        for dep in deps:
            if dep == name:
                raise ValueError("任务不能依赖自己")
            if dep not in self.tasks:
                raise KeyError("未知依赖: %s" % (dep,))
        task = Task(name, units, worker)
        task.deps = tuple(deps)
        task.created_at = self.ticks
        self.tasks[name] = task
        if gated:
            task.gated = True
            task.state = WAITING
        elif self._deps_done(task):
            task.state = READY
            self._enqueue(task)
        else:
            task.state = PENDING
        return task

    def unlock(self, name):
        """打开外部门控；依赖还没完成的任务继续等依赖。"""
        task = self._task(name)
        if not task.gated:
            raise ValueError("任务 %s 没有等待外部解锁" % name)
        task.gated = False
        if self._deps_done(task):
            task.state = READY
            self._enqueue(task)
        else:
            task.state = PENDING
        return task

    def steal(self, thief_id, victim_id):
        """让 thief 立刻从 victim 的本地队列窃取一半任务，返回被搬走的任务名。"""
        thief = self._worker(thief_id)
        victim = self._worker(victim_id)
        if thief is victim:
            raise ValueError("执行者不能从自己那里窃取")
        return [task.name for task in self._steal_from(thief, victim)]

    def step(self):
        """推进一个时间片，返回本步的动作记录；任务全部结束时返回 None。"""
        if self.done():
            return None
        self._publish_ready()
        wid = self._cursor
        self._cursor = (wid + 1) % len(self.workers)
        worker = self.workers[wid]
        self.ticks += 1
        if worker.deque:
            task = self._pop_local(worker)
            self._execute(worker, task)
            return self._record(worker, RUN, task)
        victim = self._pick_victim(wid)
        if victim is not None:
            moved = self._steal_from(worker, victim)
            if moved:
                task = self._pop_local(worker)
                self._execute(worker, task)
                return self._record(worker, STEAL, task, victim.wid)
        self.idle_ticks += 1
        return self._record(worker, IDLE, None)

    def run(self, max_steps=DEFAULT_MAX_STEPS):
        """连续推进直到所有任务结束，返回统计结果。"""
        steps = 0
        while not self.done():
            if steps >= max_steps:
                raise RuntimeError("调度在 %d 步内没有收敛" % max_steps)
            self.step()
            steps += 1
        return self.stats()

    def done(self):
        """所有任务是否都已经结束。"""
        return all(task.state is DONE for task in self.tasks.values())

    def stats(self):
        """返回调度统计结果。"""
        loads = [worker.executed for worker in self.workers]
        return {
            "ticks": self.ticks,
            "busy": self.busy_ticks,
            "idle": self.idle_ticks,
            "executed": sum(loads),
            "steal_ops": self.steal_ops,
            "stolen": self.stolen_tasks,
            "finished": sum(1 for t in self.tasks.values() if t.state is DONE),
            "peak_load": max(loads),
            "imbalance": max(loads) - min(loads),
        }

    def loads(self):
        """返回每个执行者实际执行的工作单位数。"""
        return {worker.wid: worker.executed for worker in self.workers}

    def snapshot(self):
        """返回每个执行者本地队列中的任务名，按队首到队尾排列。"""
        return {"w%d" % worker.wid: [task.name for task in worker.deque]
                for worker in self.workers}

    def locate(self, name):
        """返回任务当前所在位置：w0、w1、pending、waiting 或 done。"""
        task = self._task(name)
        for worker in self.workers:
            if any(item is task for item in worker.deque):
                return "w%d" % worker.wid
        return task.state

    # ------------------------------------------------------------ 内部实现

    def _task(self, name):
        if name not in self.tasks:
            raise KeyError("未知任务: %s" % (name,))
        return self.tasks[name]

    def _worker(self, wid):
        if isinstance(wid, bool) or not isinstance(wid, int):
            raise ValueError("执行者编号必须是整数: %r" % (wid,))
        if not 0 <= wid < len(self.workers):
            raise KeyError("未知执行者: %d" % wid)
        return self.workers[wid]

    def _deps_done(self, task):
        """任务的依赖是否已经全部完成。"""
        return all(self.tasks[dep].state is DONE for dep in task.deps)

    def _enqueue(self, task):
        """把任务放进它归属执行者的本地队列尾部。"""
        self.workers[task.worker].deque.append(task)

    def _publish_ready(self):
        """把依赖已经满足的任务放进就绪队列。"""
        for task in self.tasks.values():
            if task.state is not PENDING:
                continue
            if not self._deps_done(task):
                continue
            task.state = READY
            self._enqueue(task)

    def _pop_local(self, worker):
        """执行者从本地队列里取出下一个要跑的任务。"""
        return worker.deque.pop()

    def _pick_victim(self, wid):
        """找一个本地队列非空的受害者。"""
        count = len(self.workers)
        for offset in range(1, count):
            victim = self.workers[(wid + offset) % count]
            if victim.deque:
                return victim
        return None

    def _steal_count(self, size):
        """一次窃取搬走的任务数：一半向上取整。"""
        return (size + 1) // 2

    def _steal_from(self, thief, victim):
        """从 victim 队首搬走一半任务，放到 thief 的队尾。"""
        count = self._steal_count(len(victim.deque))
        moved = []
        for _ in range(count):
            moved.append(victim.deque.popleft())
        if not moved:
            return moved
        thief.deque.extend(moved)
        thief.steals += 1
        victim.donated += len(moved)
        self.steal_ops += 1
        self.stolen_tasks += len(moved)
        return moved

    def _execute(self, worker, task):
        """执行一个单位的工作，工作量记在实际执行的执行者名下。"""
        task.state = RUNNING
        task.remaining -= 1
        task.executions += 1
        worker.executed += 1
        self.busy_ticks += 1
        if task.remaining > 0:
            task.state = READY
            worker.deque.append(task)
        else:
            task.state = DONE
            task.finished_at = self.ticks

    def _record(self, worker, action, task, victim=None):
        """记录一步动作。"""
        record = {
            "tick": self.ticks,
            "worker": worker.wid,
            "action": action,
            "task": None if task is None else task.name,
            "victim": victim,
        }
        self.history.append(record)
        return record
