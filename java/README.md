# Tablestore for Agent Memory (Java 实现)

## 文档

- Memory Store
	- 用户与大模型交互的session会话管理和聊天消息记录
	- 使用文档:
		- [Memory Store 初始化](examples/src/main/java/com/aliyun/openservices/tablestore/agent/memory/MemoryStoreInitExample.java)
		- [Session会话管理](examples/src/main/java/com/aliyun/openservices/tablestore/agent/memory/SessionExample.java)
		- [Message消息管理](examples/src/main/java/com/aliyun/openservices/tablestore/agent/memory/MessageExample.java)
		- [与大模型聊天的实战应用](examples/src/main/java/com/aliyun/openservices/tablestore/agent/memory/SimpleUseExample.java)
- Knowledge Store
	- 知识库文档管理，相似性搜索, 常用在RAG、AI搜索、多模态搜索等领域。
	- 使用文档:
		- [Knowledge Store 初始化](examples/src/main/java/com/aliyun/openservices/tablestore/agent/knowledge/KnowledgeStoreInitExample.java)
		- [知识库管理和向量检索](examples/src/main/java/com/aliyun/openservices/tablestore/agent/knowledge/KnowledgeExample.java)
	- Spring AI Vector Store 兼容版
		- 基于 `Knowledge Store` 兼容 `Spring AI Vector Store`.
		- [TablestoreVectorStore 使用文档](examples/src/main/java/com/aliyun/openservices/tablestore/agent/spring/TablestoreVectorStoreExample.java)

## 开发

#### 依赖

- JDK 17 及以上.
  - 开发此仓库代码依赖的版本，发布到 maven 仓库后核心模块是兼容 JDK8 的，但是 SpringAI 仅支持 JDK17 的)

#### 代码格式化

请 git commit 后重新执行进行检查

```shell
make format
```

#### 测试

```shell
make test
```

#### 打包

```shell
make build
```

##### 发布 snapshots

```shell
make deploy
```
