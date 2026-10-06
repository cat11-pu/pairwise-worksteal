# pairwise-worksteal

一个只用 Python 标准库实现的工作窃取调度器内核实验项目：每个执行者有自己的本地双端队列，
调度完全由调用方以整数步驱动，不涉及真实线程、时钟和 I/O，同一份输入下行为完全确定。

```
worksteal/__init__.py     对外导出
worksteal/core.py         调度器内核
tests/test_core.py        验收测试
```

在项目根目录跑测试：

```
python3 -m unittest discover -s tests -v
```

Windows 上用任意 Python 3 解释器执行同一条命令即可，例如：

```
C:/Users/Administrator/AppData/Local/Programs/Python/Python313/python.exe -m unittest discover -s tests -v
```
