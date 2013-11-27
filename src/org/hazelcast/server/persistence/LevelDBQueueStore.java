package org.hazelcast.server.persistence;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.QueueStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class LevelDBQueueStore<T> implements LifecycleListener, QueueStore<T> {
  private final ILogger _logger = Logger.getLogger(LevelDBQueueStore.class.getName());

  private DB _db; //数据库
  private static Options _options;
  static {
    File file = new File(System.getProperty("user.dir", ".") + "/db/");
    if (!file.exists() && !file.mkdirs()) {
      throw new RuntimeException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
    }

    final org.iq80.leveldb.Logger logger = new org.iq80.leveldb.Logger() {
      ILogger logger = Logger.getLogger(LevelDBQueueStore.class.getName());

      public void log(String message) {
        logger.log(Level.INFO, message);
      }
    };

    _options = new Options().createIfMissing(true);
    _options.logger(logger);
    /*
     * LevelDB的sst文件大小默认是2M起，如果想入库时把这个搞大，只需要把options.write_buffer_size搞大，
     * 比如options.write_buffer_size = 100000000。这样一上来sst就是32M起。
     */
    _options.writeBufferSize(256 * 1024 * 1024); //log大小设成256M，这样减少切换日志的开销和减少数据合并的频率。
    _options.blockSize(256 * 1024); //256KB Block Size 
    _options.cacheSize(100 * 1024 * 1024); // 100MB cache
    _options.compressionType(CompressionType.SNAPPY);
  }

  private HazelcastInstance _hazelcastInstance;
  private String _queueName;
  private Properties _properties;

  private String getMD5OfStr(String inStr, String charset) {
    try {
      return new MD5().getMD5ofStr(inStr.getBytes(charset));
    } catch (Exception e) {
      return new MD5().getMD5ofStr(inStr);
    }
  }

  private Object byteToObject(byte[] bb) throws Exception {
    if (bb == null) {
      return null;
    } else {
      return KryoSerializer.read(bb);
    }
  }

  private byte[] objectToByte(Object object) throws Exception {
    byte[] bb = KryoSerializer.write(object);

    return bb;
  }

  public LevelDBQueueStore(Properties properties, String queueName) {
    String hazelcastInstanceName = properties.getProperty(org.hazelcast.server.HazelcastServerApp.HAZELCAST_INSTANCE_NAME, "");
    _hazelcastInstance = Hazelcast.getHazelcastInstanceByName(hazelcastInstanceName);
    _hazelcastInstance.getLifecycleService().addLifecycleListener(this);
    _properties = properties;
    _queueName = queueName;

    File dbPath = new File(System.getProperty("user.dir", ".") + "/db/" + getMD5OfStr(_queueName, org.hazelcast.server.HazelcastServerApp.DB_CHARSET));
    try {
      _db = JniDBFactory.factory.open(dbPath, _options);

      DBIterator dbIterator = _db.iterator();
      int dbCount = 0;
      for (dbIterator.seekToFirst(); dbIterator.hasNext(); dbIterator.next()) {
        dbCount++;
      }

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":count:" + dbCount);
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":Construct()完成!");
    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException("Can not start LevelDB:" + System.getProperty("user.dir", ".") + "/db/", ex);
    }
  }

  @Override
  public void stateChanged(LifecycleEvent event) {
    if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
      this.destroy();
    }
  }

  public void destroy() {
    if (_db != null) {
      DBIterator dbIterator = _db.iterator();
      int dbCount = 0;
      for (dbIterator.seekToFirst(); dbIterator.hasNext(); dbIterator.next()) {
        dbCount++;
      }
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":count:" + dbCount);

      try {
        _db.close();
      } catch (Throwable ex) {
        _logger.log(Level.WARNING, ex.getMessage(), ex);
      } finally {
        _db = null;
      }
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":destroy()完成!");
    }
  }

  @Override
  public T load(Long key) {
    try {
      byte[] bb = objectToByte(key);
      if (bb != null) {
        return (T) byteToObject(_db.get(bb));
      } else {
        return null;
      }
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void delete(Long key) {
    try {
      byte[] bb = objectToByte(key);
      if (bb != null) {
        _db.delete(bb);
      }
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  @Override
  public void deleteAll(Collection<Long> keys) {
    for (Long key : keys) {
      this.delete(key);
    }
  }

  @Override
  public void store(Long key, T value) {
    try {
      byte[] keyEntry = objectToByte(key);
      byte[] valueEntry = objectToByte(value);
      _db.put(keyEntry, valueEntry);
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  @Override
  public void storeAll(Map<Long, T> map) {
    for (Entry<Long, T> entrys : map.entrySet()) {
      this.store(entrys.getKey(), entrys.getValue());
    }
  }

  @Override
  public Map<Long, T> loadAll(Collection<Long> keys) {
    return privateLoadAll(keys);
  }

  private Map<Long, T> privateLoadAll(Collection<Long> keys) {
    Map<Long, T> map = new java.util.HashMap<Long, T>(keys.size());
    for (Long key : keys) {
      map.put(key, this.load(key));
    }

    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":loadAll:" + map.size());
    return map;
  }

  @Override
  public Set<Long> loadAllKeys() {
    return privateLoadAllKeys();
  }

  private Set<Long> privateLoadAllKeys() {
    DBIterator dbCountIterator = _db.iterator();
    int dbCount = 0;
    for (dbCountIterator.seekToFirst(); dbCountIterator.hasNext(); dbCountIterator.next()) {
      dbCount++;
    }

    Set<Long> keys = new java.util.HashSet<Long>(dbCount);
    try {
      DBIterator dbIterator = _db.iterator();
      for (dbIterator.seekToFirst(); dbIterator.hasNext(); dbIterator.next()) {
        keys.add((Long) byteToObject(dbIterator.peekNext().getKey()));
      }

    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    }

    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":loadAllKeys:" + keys.size());

    return keys;
  }

}
