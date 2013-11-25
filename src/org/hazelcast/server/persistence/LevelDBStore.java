package org.hazelcast.server.persistence;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class LevelDBStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {
  private final ILogger _logger = Logger.getLogger(LevelDBStore.class.getName());
  static final String DB_CHARSET = "UTF-8"; //���ݿ��ַ���

  private DB _db; //���ݿ�
  private static Options _options;
  private static Map<String, DB> _dbMap = new HashMap<String, DB>(); //���ݿ�Map,key��_mapName,value��DB.
  static {
    File file = new File(System.getProperty("user.dir", ".") + "/db/");
    if (!file.exists() && !file.mkdirs()) {
      throw new RuntimeException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
    }

    final org.iq80.leveldb.Logger logger = new org.iq80.leveldb.Logger() {
      ILogger logger = Logger.getLogger(LevelDBStore.class.getName());

      public void log(String message) {
        logger.log(Level.INFO, message);
      }
    };

    _options = new Options().createIfMissing(true);
    _options.logger(logger);
    /*
     * LevelDB��sst�ļ���СĬ����2M����������ʱ��������ֻ��Ҫ��options.write_buffer_size���
     * ����options.write_buffer_size = 100000000������һ����sst����32M��
     */
    _options.writeBufferSize(256 * 1024 * 1024); //log��С���256M�����������л���־�Ŀ����ͼ������ݺϲ���Ƶ�ʡ�
    _options.blockSize(256 * 1024); //256KB Block Size 
    _options.cacheSize(100 * 1024 * 1024); // 100MB cache
    _options.compressionType(CompressionType.SNAPPY);
  }

  private HazelcastInstance _hazelcastInstance;
  private String _mapName;
  private Properties _properties;

  private String getBASE64DecodeOfStr(String inStr, String charset) {
    try {
      return new String(Base64.decode(inStr), charset);
    } catch (UnsupportedEncodingException e) {
      return new String(Base64.decode(inStr));
    }
  }

  private Object byteToObject(byte[] bb) throws Exception {
    return KryoSerializer.read(bb);
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

    File dbPath = new File(System.getProperty("user.dir", ".") + "/db/" + getBASE64DecodeOfStr(_mapName, DB_CHARSET));
    try {
      _db = JniDBFactory.factory.open(dbPath, _options);
      _dbMap.put(_mapName, _db);

      java.util.Iterator<java.util.Map.Entry<byte[], byte[]>> dbIterator = _db.iterator();
      int dbCount = 0;
      while (dbIterator.hasNext()) {
        dbIterator.next();
        dbCount++;
      }

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":count:" + dbCount);
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":��ʼ�����!");

      //Ԥ�Ȱ����ݼ��ؽ�Hazelcast��Ⱥ��
      IMap<K, V> map = _hazelcastInstance.getMap(mapName);
      Set<K> keySet = privateLoadAllKeys();
      for (K key : keySet) {
        map.putTransient(key, load(key), 0, TimeUnit.SECONDS);
      }
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":Ԥ�ȼ����������!");

    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException("Can not start LevelDB:" + System.getProperty("user.dir", ".") + "/db/", ex);
    }
  }

  @Override
  public void destroy() {
    if (_db != null) {
      java.util.Iterator<java.util.Map.Entry<byte[], byte[]>> dbIterator = _db.iterator();
      int dbCount = 0;
      while (dbIterator.hasNext()) {
        dbIterator.next();
        dbCount++;
      }
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":count:" + dbCount);

      try {
        _db.close();
      } catch (Throwable ex) {
        _logger.log(Level.WARNING, ex.getMessage(), ex);
      } finally {
        _db = null;
        _dbMap.remove(_mapName);
      }
      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":�������!");
    }
  }

  @Override
  public V load(K key) {
    try {
      byte[] bb = objectToByte(key);
      return (V) byteToObject(_db.get(bb));
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void delete(K key) {
    try {
      _db.delete(objectToByte(key));
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
  public Map<K, V> loadAll(Collection<K> keys) {
    //return privateLoadAll(keys);

    return null;
  }

  private Map<K, V> privateLoadAll(Collection<K> keys) {
    Map<K, V> map = new java.util.HashMap<K, V>(keys.size());
    for (K key : keys) {
      map.put(key, this.load(key));
    }
    return map;
  }

  @Override
  public Set<K> loadAllKeys() {
    //return privateLoadAllKeys();

    return null;
  }

  private Set<K> privateLoadAllKeys() {
    java.util.Iterator<java.util.Map.Entry<byte[], byte[]>> dbCountIterator = _db.iterator();
    int dbCount = 0;
    while (dbCountIterator.hasNext()) {
      dbCountIterator.next();
      dbCount++;
    }

    Set<K> keys = new java.util.HashSet<K>(dbCount);
    try {
      java.util.Iterator<java.util.Map.Entry<byte[], byte[]>> dbIterator = _db.iterator();
      while (dbIterator.hasNext()) {
        java.util.Map.Entry<byte[], byte[]> entt = dbIterator.next();
        keys.add((K) byteToObject(entt.getKey()));
      }

    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
    }

    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":loadAllKeys:" + keys.size());

    return keys;
  }
}
