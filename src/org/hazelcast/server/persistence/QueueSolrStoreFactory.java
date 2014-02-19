package org.hazelcast.server.persistence;

import java.util.Properties;

import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;

public class QueueSolrStoreFactory<T> implements QueueStoreFactory<T> {

  @Override
  public QueueStore<T> newQueueStore(String name, Properties properties) {
    return new QueueSolrStore<T>(properties, name);
  }

}
