"""worksteal.core 的验收测试。

只写期望行为，覆盖正常流程、边界、异常输入、回归场景与调度不变量。
"""

import unittest

from worksteal import (
    DONE,
    PENDING,
    RUN,
    STEAL,
    WAITING,
    WorkStealingScheduler,
)


def run_until_done(sched, limit=300):
    """推进到所有任务结束。"""
    for _ in range(limit):
        if sched.done():
            return
        sched.step()
    raise AssertionError("调度没有在 %d 步内收敛" % limit)


def queued_names(sched):
    """当前排在各执行者本地队列里的任务名。"""
    names = []
    for tasks in sched.snapshot().values():
        names.extend(tasks)
    return names


def executed_by_worker(sched):
    """从动作记录里统计每个执行者真正执行了多少个工作单位。"""
    counts = {worker.wid: 0 for worker in sched.workers}
    for record in sched.history:
        if record["action"] in (RUN, STEAL):
            counts[record["worker"]] += 1
    return counts


class WorkStealingTests(unittest.TestCase):

    def test_local_queue_pop_is_lifo(self):
        sched = WorkStealingScheduler(workers=1)
        for name in ("a", "b", "c"):
            sched.add(name, 1, worker=0)

        run_until_done(sched)

        self.assertEqual([record["task"] for record in sched.history
                          if record["action"] == RUN], ["c", "b", "a"])
        self.assertEqual(sched.snapshot(), {"w0": []})
        self.assertEqual(sched.stats()["executed"], 3)

    def test_steal_moves_the_oldest_half(self):
        sched = WorkStealingScheduler(workers=2)
        for name in ("t1", "t2", "t3", "t4", "t5"):
            sched.add(name, 2, worker=0)

        moved = sched.steal(1, 0)

        self.assertEqual(moved, ["t1", "t2", "t3"])
        self.assertEqual(sched.snapshot(), {"w0": ["t4", "t5"],
                                            "w1": ["t1", "t2", "t3"]})
        self.assertEqual(sched.tasks["t1"].worker, 0)
        self.assertEqual(sched.workers[1].steals, 1)
        self.assertEqual(sched.workers[0].donated, 3)
        self.assertEqual(sched.stats()["steal_ops"], 1)
        self.assertEqual(sched.stats()["stolen"], 3)

        run_until_done(sched)
        self.assertEqual([task.executions for task in sched.tasks.values()],
                         [2, 2, 2, 2, 2])
        self.assertEqual(sched.loads(), executed_by_worker(sched))

    def test_steal_boundaries_and_input_errors(self):
        sched = WorkStealingScheduler(workers=2)
        sched.add("solo", 2, worker=1)

        self.assertEqual(sched.steal(0, 1), ["solo"])
        self.assertEqual(sched.snapshot(), {"w0": ["solo"], "w1": []})
        self.assertEqual(sched.steal(0, 1), [])
        self.assertEqual(sched.stats()["steal_ops"], 1)
        self.assertEqual(sched.stats()["stolen"], 1)
        self.assertEqual(sched.workers[0].donated, 0)

        self.assertRaises(ValueError, sched.steal, 0, 0)
        self.assertRaises(KeyError, sched.steal, 0, 7)
        self.assertRaises(ValueError, WorkStealingScheduler, 0)
        self.assertRaises(KeyError, sched.locate, "ghost")
        self.assertRaises(ValueError, sched.add, "solo", 1)
        self.assertRaises(ValueError, sched.add, "bad", 0)
        self.assertRaises(ValueError, sched.add, "selfish", 1, ("selfish",))
        self.assertRaises(KeyError, sched.add, "later", 1, ("missing",))

        run_until_done(sched)
        self.assertEqual(sched.tasks["solo"].executions, 2)

    def test_dependency_publishes_only_after_all_deps_done(self):
        sched = WorkStealingScheduler(workers=2)
        sched.add("a", 1, worker=0)
        sched.add("b", 4, worker=0)
        sched.add("c", 1, deps=("a", "b"), worker=1)
        self.assertEqual(sched.locate("c"), PENDING)

        first_c = None
        for _ in range(200):
            if sched.done():
                break
            sched.step()
            if first_c is None and sched.tasks["c"].executions > 0:
                first_c = sched.ticks
            if sched.tasks["a"].state is DONE and sched.tasks["b"].state is not DONE:
                self.assertNotIn("c", queued_names(sched))

        self.assertTrue(sched.done())
        self.assertIsNotNone(first_c)
        self.assertGreater(first_c, sched.tasks["b"].finished_at)
        self.assertEqual([task.executions for task in sched.tasks.values()],
                         [1, 4, 1])

    def test_task_enters_its_home_worker_queue(self):
        sched = WorkStealingScheduler(workers=3)
        sched.add("bulk", 8, worker=0)
        sched.add("dep", 1, worker=1)
        sched.add("fanin", 1, deps=("dep",), worker=1)

        self.assertEqual(sched.locate("bulk"), "w0")
        self.assertEqual(sched.locate("dep"), "w1")
        self.assertEqual(sched.locate("fanin"), PENDING)
        self.assertEqual(sched.snapshot(), {"w0": ["bulk"], "w1": ["dep"], "w2": []})

        sched.step()
        sched.step()
        self.assertEqual(sched.tasks["dep"].state, DONE)
        sched.step()
        self.assertEqual(sched.locate("fanin"), "w1")

        run_until_done(sched)
        self.assertEqual([task.executions for task in sched.tasks.values()],
                         [8, 1, 1])
        self.assertEqual(sched.idle_ticks, 0)
        self.assertEqual(sched.busy_ticks, 10)

    def test_load_is_credited_to_the_worker_that_ran_the_task(self):
        sched = WorkStealingScheduler(workers=2)
        sched.add("big", 4, worker=0)
        sched.add("small", 2, worker=0)

        run_until_done(sched)

        actual = executed_by_worker(sched)
        self.assertEqual(sched.loads(), actual)
        self.assertGreater(actual[1], 0)
        self.assertEqual(sched.stats()["peak_load"], max(actual.values()))
        self.assertEqual(sched.stats()["imbalance"],
                         max(actual.values()) - min(actual.values()))
        self.assertEqual(sum(actual.values()), sched.busy_ticks)
        self.assertEqual(sched.stats()["executed"], sched.busy_ticks)

    def test_idle_slots_only_when_nothing_is_runnable(self):
        sched = WorkStealingScheduler(workers=2)
        for name in ("a", "b", "c"):
            sched.add(name, 1, worker=0)

        run_until_done(sched)

        self.assertEqual(sched.idle_ticks, 0)
        self.assertEqual(sched.busy_ticks, 3)
        self.assertEqual(sched.stats()["ticks"],
                         sched.busy_ticks + sched.idle_ticks)

        gated = WorkStealingScheduler(workers=1)
        gated.add("g", 1, worker=0, gated=True)
        for _ in range(3):
            gated.step()
        self.assertEqual(gated.idle_ticks, 3)
        self.assertEqual(gated.busy_ticks, 0)
        self.assertEqual(gated.stats()["ticks"], 3)

        gated.unlock("g")
        run_until_done(gated)
        self.assertEqual(gated.tasks["g"].executions, 1)
        self.assertEqual(gated.stats()["ticks"],
                         gated.busy_ticks + gated.idle_ticks)

    def test_empty_worker_reaches_work_across_workers(self):
        sched = WorkStealingScheduler(workers=3)
        for name in ("p", "q", "r", "s"):
            sched.add(name, 2, worker=0)

        run_until_done(sched)

        actual = executed_by_worker(sched)
        self.assertEqual(sched.idle_ticks, 0)
        self.assertEqual(sched.busy_ticks, 8)
        self.assertEqual(sched.stats()["ticks"], 8)
        self.assertEqual(sched.loads(), actual)
        self.assertGreaterEqual(sched.workers[1].steals, 1)
        self.assertGreaterEqual(sched.workers[2].steals, 1)
        self.assertEqual([task.executions for task in sched.tasks.values()],
                         [2, 2, 2, 2])

    def test_unlock_gate_and_accounting(self):
        sched = WorkStealingScheduler(workers=2)
        sched.add("g", 2, worker=1, gated=True)
        self.assertEqual(sched.locate("g"), WAITING)

        for _ in range(3):
            sched.step()
        self.assertEqual(sched.idle_ticks, 3)
        self.assertEqual(sched.busy_ticks, 0)
        self.assertEqual(sched.stats()["ticks"], 3)
        self.assertFalse(sched.done())

        sched.unlock("g")
        self.assertEqual(sched.locate("g"), "w1")
        self.assertRaises(ValueError, sched.unlock, "g")
        self.assertRaises(KeyError, sched.unlock, "ghost")

        run_until_done(sched)
        self.assertEqual(sched.tasks["g"].executions, 2)
        self.assertEqual(sched.stats()["ticks"], 5)
        self.assertEqual(sched.stats()["busy"], 2)
        self.assertEqual(sched.stats()["idle"], 3)

    def test_end_to_end_order_and_accounting(self):
        sched = WorkStealingScheduler(workers=3)
        sched.add("root", 3, worker=0)
        sched.add("side", 1, worker=1)
        sched.add("left", 2, deps=("root",), worker=1)
        sched.add("right", 4, deps=("root",), worker=2)
        sched.add("join", 2, deps=("left", "right"), worker=0)

        for _ in range(100):
            if sched.done():
                break
            sched.step()
            names = queued_names(sched)
            self.assertEqual(len(names), len(set(names)))
            for task in sched.tasks.values():
                self.assertLessEqual(task.executions, task.units)

        self.assertTrue(sched.done())
        self.assertEqual([record["task"] for record in sched.history
                          if record["action"] in (RUN, STEAL)],
                         ["root", "side", "root", "root", "left", "right",
                          "left", "right", "right", "right", "join", "join"])
        self.assertEqual([task.executions for task in sched.tasks.values()],
                         [3, 1, 2, 4, 2])
        self.assertEqual(sched.ticks, 12)
        self.assertEqual(sched.idle_ticks, 0)
        self.assertEqual(sched.busy_ticks, 12)
        self.assertEqual(sched.loads(), {0: 4, 1: 4, 2: 4})
        self.assertEqual(sched.stats()["stolen"], 8)
        self.assertEqual(sched.stats()["steal_ops"], 8)
        self.assertEqual(sched.tasks["root"].finished_at, 4)
        self.assertEqual(sched.tasks["join"].finished_at, 12)
        self.assertLess(sched.tasks["root"].finished_at,
                        sched.tasks["left"].finished_at)
        self.assertLess(sched.tasks["root"].finished_at,
                        sched.tasks["right"].finished_at)
        self.assertLess(sched.tasks["left"].finished_at,
                        sched.tasks["join"].finished_at)
        self.assertLess(sched.tasks["right"].finished_at,
                        sched.tasks["join"].finished_at)


if __name__ == "__main__":
    unittest.main()
