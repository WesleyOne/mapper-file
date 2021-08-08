> 本项目参照[RocketMQ](https://github.com/apache/rocketmq) 和[dledger](https://github.com/openmessaging/dledger) 项目的存储模块。

实现以下功能：
- 对象序列化后存储到本地文件
- 从本地文件读取对象。

_核心技术是用到`NIO`的`MappedByteBuffer`内存文件映射字节缓冲区，提高文件读写能力。_