package org.hazelcast.server.persistence;

import java.util.Properties;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreFactory;

public class MapSolrStoreFactory<K, V> implements MapStoreFactory<K, V> {

  @Override
  public MapLoader<K, V> newMapStore(String mapName, Properties properties) {
    return new MapSolrStore<K, V>(mapName, properties);
  }

}
