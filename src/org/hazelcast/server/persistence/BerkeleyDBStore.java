package org.hazelcast.server.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

@SuppressWarnings("unchecked")
public class BerkeleyDBStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V>, Runnable {
  private final ILogger _logger = Logger.getLogger(BerkeleyDBStore.class.getName());

  private Environment _env;
  private Database _db; //数据库

  private ScheduledExecutorService _scheduleSync; //同步磁盘的Scheduled

  private HazelcastInstance _hazelcastInstance;
  private Properties _properties;
  private String _mapName;

  private Object entryToObject(DatabaseEntry entry) throws Exception {
    int len = entry.getSize();
    if (len == 0) {
      return null;
    } else {
      ByteArrayInputStream bais = new ByteArrayInputStream(entry.getData());
      JBossObjectInputStream ois = new JBossObjectInputStream(bais);
      return ois.readObject();
    }
  }

  private DatabaseEntry objectToEntry(Object object) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JBossObjectOutputStream oos = new JBossObjectOutputStream(baos);
    oos.writeObject(object);

    DatabaseEntry entry = new DatabaseEntry();
    entry.setData(baos.toByteArray());
    return entry;
  }

  @Override
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    _hazelcastInstance = hazelcastInstance;
    _properties = properties;
    _mapName = mapName;

    if (_env == null) {
      EnvironmentConfig envConfig = new EnvironmentConfig();
      envConfig.setAllowCreate(true);
      envConfig.setLocking(true); //true时让Cleaner Thread自动启动,来清理废弃的数据库文件.
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

    if (_scheduleSync == null) {
      int syncinterval = 3;
      try {
        syncinterval = Integer.parseInt(_properties.getProperty("syncinterval"));
      } catch (Exception e) {
        _logger.log(Level.WARNING, e.getMessage(), e);
      }
      _scheduleSync = Executors.newSingleThreadScheduledExecutor(); //同步磁盘的Scheduled
      _scheduleSync.scheduleWithFixedDelay(this, 1, syncinterval, TimeUnit.SECONDS);
    }
    System.out.println(this.getClass().getCanonicalName() + ":" + _mapName + ":初始化完成!");
  }

  @Override
  public void destroy() {
    if (_scheduleSync != null) {
      try {
        _scheduleSync.shutdown();
      } finally {
        _scheduleSync = null;
      }
    }

    if (_db != null) {
      try {
        _db.sync();
      } catch (Throwable ex) {
        _logger.log(Level.WARNING, ex.getMessage(), ex);
      }

      try {
        _db.close();
      } catch (Throwable ex) {
        _logger.log(Level.WARNING, ex.getMessage(), ex);
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
        _logger.log(Level.WARNING, ex.getMessage(), ex);
      }

      try {
        _env.close();
      } catch (Throwable ex) {
        _logger.log(Level.WARNING, ex.getMessage(), ex);
      } finally {
        _env = null;
      }
    }

    System.out.println(this.getClass().getCanonicalName() + ":" + _mapName + ":销毁完成!");
  }

  @Override
  //定时将内存中的内容写入磁盘
  public void run() {
    try {
      _db.sync();
    } catch (Throwable ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
    }
  }

  @Override
  public V load(K key) {
    try {
      DatabaseEntry keyEntry = objectToEntry(key);
      DatabaseEntry valueEntry = new DatabaseEntry();
      OperationStatus status = _db.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
      if (status == OperationStatus.SUCCESS) {
        return (V) entryToObject(valueEntry);
      } else {
        return null;
      }
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  @Override
  public Map<K, V> loadAll(Collection<K> keys) {
    Map<K, V> map = new java.util.HashMap<K, V>(keys.size());
    for (K key : keys) {
      map.put(key, this.load(key));
    }
    return map;
  }

  @Override
  public Set<K> loadAllKeys() {
    Set<K> keys = new java.util.HashSet<K>((int) _db.count());
    Cursor cursor = null;
    try {
      cursor = _db.openCursor(null, null);
      DatabaseEntry foundKey = new DatabaseEntry();
      DatabaseEntry foundData = new DatabaseEntry();

      while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        keys.add((K) entryToObject(foundKey));
      }
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    } finally {
      cursor.close();
    }
    return keys;
  }

  @Override
  public void delete(K key) {
    try {
      _db.delete(null, objectToEntry(key));
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  @Override
  public void deleteAll(Collection<K> keys) {
    for (K key : keys) {
      this.delete(key);
    }
  }

  @Override
  public void store(K key, V value) {
    try {
      DatabaseEntry keyEntry = objectToEntry(key);
      DatabaseEntry valueEntry = objectToEntry(value);
      _db.put(null, keyEntry, valueEntry);
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  @Override
  public void storeAll(Map<K, V> map) {
    for (Entry<K, V> entrys : map.entrySet()) {
      this.store(entrys.getKey(), entrys.getValue());
    }
  }

}
