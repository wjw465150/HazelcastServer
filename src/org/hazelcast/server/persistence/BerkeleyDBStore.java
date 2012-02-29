package org.hazelcast.server.persistence;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BerkeleyDBStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V>, Runnable {
  private Environment _env;
  private Database _db; //���ݿ�

  private ScheduledExecutorService _scheduleSync = Executors.newSingleThreadScheduledExecutor(); //ͬ�����̵�Scheduled

  private HazelcastInstance _hazelcastInstance;
  private Properties _properties;
  private String _mapName;
  private Map<K, V> _map;

  @Override
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    _hazelcastInstance = hazelcastInstance;
    _properties = properties;
    _mapName = mapName;

    if (_env == null) {
      EnvironmentConfig envConfig = new EnvironmentConfig();
      envConfig.setAllowCreate(true);
      envConfig.setLocking(true);   //trueʱ��Cleaner Thread�Զ�����,��������������ݿ��ļ�.
      envConfig.setSharedCache(true);
      envConfig.setTransactional(false);
      envConfig.setCachePercent(10); //����Ҫ,�����ʵ�ֵ�ή���ٶ�
      envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "104857600"); //����log��־�ļ��ߴ���100M

      File file = new File(System.getProperty("user.dir", ".") + "/db/");
      if (!file.exists() && !file.mkdirs()) {
        throw new RuntimeException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
      }
      _env = new Environment(file, envConfig);
    }

    if (_db == null) {
      DatabaseConfig dbConfig = new DatabaseConfig();
      dbConfig.setAllowCreate(true);
      dbConfig.setDeferredWrite(true); //�ӳ�д
      dbConfig.setSortedDuplicates(false);
      dbConfig.setTransactional(false);
      _db = _env.openDatabase(null, _mapName, dbConfig);
    }

    if (_map == null) {
      ObjectBinding<K> keyBinding = new ObjectBinding<K>();
      ObjectBinding<V> dataBinding = new ObjectBinding<V>();
      this._map = new StoredMap<K, V>(_db, keyBinding, dataBinding, true);
    }

    int syncinterval = 3;
    try {
      syncinterval = Integer.parseInt(_properties.getProperty("syncinterval"));
    } catch (Exception e) {
      //e.printStackTrace();
    }
    _scheduleSync.scheduleWithFixedDelay(this, 1, syncinterval, TimeUnit.SECONDS);
    System.out.println(this.getClass().getCanonicalName() + ":" + _mapName + ":��ʼ�����!");
  }

  @Override
  public void destroy() {
    _scheduleSync.shutdown();

    if (_db != null) {
      try {
        _db.sync();
      } catch (Throwable ex) {
        ex.printStackTrace();
      }

      try {
        _db.close();
      } catch (Throwable ex) {
        ex.printStackTrace();
      } finally {
        _db = null;
      }
    }

    if (_env != null) {
      try {
        boolean anyCleaned = false;
        while (_env.cleanLog() > 0) {
          anyCleaned = true;
        }
        if (anyCleaned) {
          CheckpointConfig force = new CheckpointConfig();
          force.setForce(true);
          _env.checkpoint(force);
        }
      } catch (Throwable ex) {
        ex.printStackTrace();
      }

      try {
        _env.close();
      } catch (Throwable ex) {
        ex.printStackTrace();
      } finally {
        _env = null;
      }
    }

    System.out.println(this.getClass().getCanonicalName() + ":" + _mapName + ":�������!");
  }

  @Override
  //��ʱ���ڴ��е�����д�����
  public void run() {
    try {
      _db.sync();
    } catch (Throwable ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public V load(K key) {
    return this._map.get(key);
  }

  @Override
  public Map<K, V> loadAll(Collection<K> keys) {
    Map<K, V> map = new java.util.HashMap<K, V>(keys.size());
    for (K key : keys) {
      map.put(key, this._map.get(key));
    }
    return map;
  }

  @Override
  public Set<K> loadAllKeys() {
    return this._map.keySet();
  }

  @Override
  public void delete(K key) {
    this._map.remove(key);
  }

  @Override
  public void deleteAll(Collection<K> keys) {
    for (K key : keys) {
      this._map.remove(key);
    }
  }

  @Override
  public void store(K key, V value) {
    this._map.put(key, value);
  }

  @Override
  public void storeAll(Map<K, V> map) {
    this._map.putAll(map);
  }

}
