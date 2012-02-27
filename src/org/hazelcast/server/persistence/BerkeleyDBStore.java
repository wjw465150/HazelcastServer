package org.hazelcast.server.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.sleepycat.bind.ByteArrayBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BerkeleyDBStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {
  private Environment _env;
  private Database _db; //数据库

  private HazelcastInstance _hazelcastInstance;
  private Properties _properties;
  private String _mapName;
  private Map<K, V> _map;

  private byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JBossObjectOutputStream oos = new JBossObjectOutputStream(baos);
    oos.writeObject(obj);
    return baos.toByteArray();
  }

  private Object deserialize(byte[] bb) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bb);
    JBossObjectInputStream ois = new JBossObjectInputStream(bais);
    return ois.readObject();

  }

  @Override
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    _hazelcastInstance = hazelcastInstance;
    _properties = properties;
    _mapName = mapName;

    synchronized (BerkeleyDBStore.class) {
      if (_env == null) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setLocking(false);
        envConfig.setSharedCache(true);
        envConfig.setTransactional(false);
        envConfig.setCachePercent(10); //很重要,不合适的值会降低速度
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "104857600"); //单个log日志文件尺寸是100M

        File file = new File(System.getProperty("user.dir", ".") + "/db/");
        if (!file.exists() && !file.mkdirs()) {
          throw new RuntimeException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
        }
        _env = new Environment(file, envConfig);
      }

      if (_db == null) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true); //延迟写
        dbConfig.setSortedDuplicates(false);
        dbConfig.setTransactional(false);
        _db = _env.openDatabase(null, _mapName, dbConfig);
      }

      if (_map == null) {
        ByteArrayBinding keyBinding = new ByteArrayBinding();
        ByteArrayBinding dataBinding = new ByteArrayBinding();
        this._map = new StoredMap<byte[], byte[]>(_db, keyBinding, dataBinding, true);
      }

    }
  }

  @Override
  public void destroy() {
    synchronized (BerkeleyDBStore.class) {
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
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public V load(K key) {
    return this._map.get(key);
    try {
      DatabaseEntry entryKey = new DatabaseEntry(serialize(key));
      DatabaseEntry entryValue = new DatabaseEntry();

      OperationStatus status = _db.get(null, entryKey, entryValue, LockMode.DEFAULT);
      if (status == OperationStatus.SUCCESS) {
        return (V) deserialize(entryValue.getData());
      } else {
        return null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public Map<K, V> loadAll(Collection<K> keys) {
    Map<K, V> map = new java.util.HashMap<K, V>(keys.size());
    for (K key : keys) {
      V value = this.load(key);
      if (value != null) {
        map.put(key, value);
      }
    }
    return map;
  }

  @Override
  public Set<K> loadAllKeys() {
    return null;
  }

  @Override
  public void delete(K key) {
  }

  @Override
  public void deleteAll(Collection<K> keys) {
  }

  @Override
  public void store(K key, V value) {
  }

  @Override
  public void storeAll(Map<K, V> map) {
  }

}
