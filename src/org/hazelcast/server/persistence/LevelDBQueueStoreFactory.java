package org.hazelcast.server.persistence;

import java.util.Properties;

import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;

public class LevelDBQueueStoreFactory<T> implements QueueStoreFactory<T> {

  @Override
  public QueueStore<T> newQueueStore(String name, Properties properties) {
    return new LevelDBQueueStore<T>(properties, name);
  }

}
