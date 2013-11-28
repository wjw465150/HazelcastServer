package org.hazelcast.server.persistence;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;

public class LevelDBQueueStoreFactory<T> implements QueueStoreFactory<T> {
  @SuppressWarnings("rawtypes")
  static final Map<String, QueueStore> queueStoreMap = new HashMap<String, QueueStore>();

  @SuppressWarnings("unchecked")
  @Override
  public QueueStore<T> newQueueStore(String name, Properties properties) {
    synchronized (LevelDBQueueStoreFactory.class) {
      if (queueStoreMap.containsKey(name) == false) {
        QueueStore<T> levelDBQueueStore = new LevelDBQueueStore<T>(properties, name);
        queueStoreMap.put(name, levelDBQueueStore);
      }
      return queueStoreMap.get(name);
    }
  }

}
