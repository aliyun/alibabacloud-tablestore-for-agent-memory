# Tablestore for Agent Memory (Python 实现)

主要包括2个实现：

- Memory Store
  - 用户与大模型交互的session会话管理和聊天消息记录 
  - [入门指南链接](docs/memory_store_demo.ipynb)
- Knowledge Store


## 依赖

- python >= 3.9
- 项目管理工具：poetry

## 打包

```shell
  poetry build
```

打包结果在当前目录`dist`下.

别的项目本地安装该项目的引用:
```shell
 pip install ${真实目录}/dist/tablestore_for_agent_memory-${具体版本}-py3-none-any.whl
```