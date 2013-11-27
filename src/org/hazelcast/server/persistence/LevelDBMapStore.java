package org.hazelcast.server.persistence;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class LevelDBMapStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {
  private final ILogger _logger = Logger.getLogger(LevelDBMapStore.class.getName());

  private DB _db; //数据库
  private static Options _options;
  static {
    File file = new File(System.getProperty("user.dir", ".") + "/db/");
    if (!file.exists() && !file.mkdirs()) {
      throw new RuntimeException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
    }

    final org.iq80.leveldb.Logger logger = new org.iq80.leveldb.Logger() {
      ILogger logger = Logger.getLogger(LevelDBMapStore.class.getName());

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
  private String _mapName;
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

  @Override
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    _hazelcastInstance = hazelcastInstance;
    _properties = properties;
    _mapName = mapName;

    File dbPath = new File(System.getProperty("user.dir", ".") + "/db/" + getMD5OfStr(_mapName, org.hazelcast.server.HazelcastServerApp.DB_CHARSET));
    try {
      _db = JniDBFactory.factory.open(dbPath, _options);

      DBIterator dbIterator = _db.iterator();
      int dbCount = 0;
      for (dbIterator.seekToFirst(); dbIterator.hasNext(); dbIterator.next()) {
        dbCount++;
      }

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":count:" + dbCount);
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":init()完成!");
    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException("Can not start LevelDB:" + System.getProperty("user.dir", ".") + "/db/", ex);
    }
  }

  @Override
  public void destroy() {
    if (_db != null) {
      DBIterator dbIterator = _db.iterator();
      int dbCount = 0;
      for (dbIterator.seekToFirst(); dbIterator.hasNext(); dbIterator.next()) {
        dbCount++;
      }
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":count:" + dbCount);

      try {
        _db.close();
      } catch (Throwable ex) {
        _logger.log(Level.WARNING, ex.getMessage(), ex);
      } finally {
        _db = null;
      }
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":destroy()完成!");
    }
  }

  @Override
  public V load(K key) {
    try {
      byte[] bb = objectToByte(key);
      if (bb != null) {
        return (V) byteToObject(_db.get(bb));
      } else {
        return null;
      }
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void delete(K key) {
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
  public void deleteAll(Collection<K> keys) {
    for (K key : keys) {
      this.delete(key);
    }
  }

  @Override
  public void store(K key, V value) {
    try {
      byte[] keyEntry = objectToByte(key);
      byte[] valueEntry = objectToByte(value);
      _db.put(keyEntry, valueEntry);
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

  @Override
  public Map<K, V> loadAll(Collection<K> keys) { //@wjw_note: 由于Hazelcast的BUG,此处必须返回null
    return privateLoadAll(keys);
    
//    IMap<K, V> imap = _hazelcastInstance.getMap(_mapName);
//    Map<K, V> map = privateLoadAll(keys);
//    for (Map.Entry<K, V> et : map.entrySet()) {
//      imap.putTransient(et.getKey(), et.getValue(), 0, TimeUnit.SECONDS);
//    }
//    return null;
  }

  private Map<K, V> privateLoadAll(Collection<K> keys) {
    Map<K, V> map = new java.util.HashMap<K, V>(keys.size());
    for (K key : keys) {
      map.put(key, this.load(key));
    }

    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":loadAll:" + map.size());
    return map;
  }

  @Override
  public Set<K> loadAllKeys() { //@wjw_note: 由于Hazelcast的BUG,此处必须返回null
    return privateLoadAllKeys();
    
//    IMap<K, V> map = _hazelcastInstance.getMap(_mapName);
//    Set<K> keySet = privateLoadAllKeys();
//    for (K key : keySet) {
//      map.putTransient(key, load(key), 0, TimeUnit.SECONDS);
//    }
//
//    return null;
  }

  private Set<K> privateLoadAllKeys() {
    DBIterator dbCountIterator = _db.iterator();
    int dbCount = 0;
    for (dbCountIterator.seekToFirst(); dbCountIterator.hasNext(); dbCountIterator.next()) {
      dbCount++;
    }

    Set<K> keys = new java.util.HashSet<K>(dbCount);
    try {
      DBIterator dbIterator = _db.iterator();
      for (dbIterator.seekToFirst(); dbIterator.hasNext(); dbIterator.next()) {
        keys.add((K) byteToObject(dbIterator.peekNext().getKey()));
      }

    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    }

    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":loadAllKeys:" + keys.size());

    return keys;
  }

}
