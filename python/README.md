# Tablestore for Agent Memory (Python 实现)

## 文档 

- Memory Store
  - 用户与大模型交互的session会话管理和聊天消息记录 
  - [入门指南 notebook 链接](docs/memory_store_tutorial.ipynb)
- Knowledge Store
  - 知识库文档管理，相似性搜索。
  - [入门指南 notebook 链接](docs/knowledge_store_tutorial.ipynb)


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
  # pip 项目
  pip install ${真实目录}/dist/tablestore_for_agent_memory-${具体版本}-py3-none-any.whl
  # poetry 项目
  poetry add ${真实目录}/dist/tablestore_for_agent_memory-${具体版本}-py3-none-any.whl
```

## 开发

安装依赖：
```shell
  poetry install
```
代码格式化：
```shell
  make format
```
跑全部测试：
```shell
  make test
```