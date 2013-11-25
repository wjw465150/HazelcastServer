HazelcastServer是基于开源项目[Hazelcast](https://github.com/hazelcast/hazelcast) `(https://github.com/hazelcast/hazelcast)`一个高度可扩展的基于P2P的云计算平台。特性包括：  
>
- [x] 提供java.util.{Queue, Set, List, Map}分布式实现。  
- [x] 提供java.util.concurrency.locks.Lock分布式全局锁实现。  
- [x] 提供java.util.concurrent.ExecutorService分布式实现。  
- [x] 提供用于一对多关系的分布式MultiMap。  
- [x] 提供用于发布/订阅的分布式Topic（主题）。  
- [x] 通过JCA与J2EE容器集成和事务支持。  
- [x] 提供用于安全集群的Socket层加密。  
- [x] 为Hibernate提供二级缓存Provider 。  
- [x] 通过JMX监控和管理集群。  
- [x] 支持动态HTTP Session集群。  
- [x] 利用备份实现动态分割。  
- [x] 支持动态故障恢复,动态节点添加,删除。  
- [x] 支持内存数据的持久化存储,保证365天的无故障运行  
- [x] 高达3万/秒的快速存取.  
>

###### HazelcastServer的扩展:
>
- [x] 使用LevelDB或者BerkeleyDB来持久化Map,防止意外当机引起的数据丢失!
