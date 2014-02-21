package org.hazelcast.server.persistence;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import org.wjw.efjson.JsonObject;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class MapSolrStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {
  private final ILogger _logger = Logger.getLogger(MapSolrStore.class.getName());

  private int connectTimeout = 60 * 1000; //连接超时
  private int readTimeout = 60 * 1000; //读超时

  private String[] urlGets;
  private String[] urlUpdates;

  private String _mapName;
  private Properties _properties;

  private Lock lockGet = new ReentrantLock();
  private int indexGet = -1;
  private Lock lockPost = new ReentrantLock();
  private int indexPost = -1;

  @Override
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    _properties = properties;
    _mapName = mapName;

    try {
      if (_properties.getProperty("servers") == null) {
        throw new RuntimeException("propertie Solr 'servers' Can not null");
      }
      String[] urlArray = _properties.getProperty("servers").split(",");
      this.urlGets = new String[urlArray.length];
      for (int i = 0; i < urlArray.length; i++) {
        if (urlArray[i].endsWith("/")) {
          urlGets[i] = urlArray[i] + "get?id=";
        } else {
          urlGets[i] = urlArray[i] + "/get?id=";
        }
      }

      this.urlUpdates = new String[urlArray.length];
      for (int i = 0; i < urlArray.length; i++) {
        if (urlArray[i].endsWith("/")) {
          urlUpdates[i] = urlArray[i] + "update";
        } else {
          urlUpdates[i] = urlArray[i] + "/update";
        }
      }

      if (_properties.getProperty("connectTimeout") != null) {
        connectTimeout = Integer.parseInt(_properties.getProperty("connectTimeout"));
      }
      if (_properties.getProperty("readTimeout") != null) {
        readTimeout = Integer.parseInt(_properties.getProperty("readTimeout"));
      }

      SolrTools.getDoc(urlGets[0], connectTimeout, readTimeout, "0");

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":init()完成!");
    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void destroy() {
    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":destroy()完成!");
  }

  public String getSolrGetUrl() {
    if (urlGets.length == 1) {
      return urlGets[0];
    }

    lockGet.lock();
    try {
      indexGet++;
      if (indexGet >= urlGets.length) {
        indexGet = 0;
      }

      return urlGets[indexGet];
    } finally {
      lockGet.unlock();
    }
  }

  public String getSolrUpdateUrl() {
    if (urlUpdates.length == 1) {
      return urlUpdates[0];
    }

    lockPost.lock();
    try {
      indexPost++;
      if (indexPost >= urlUpdates.length) {
        indexPost = 0;
      }

      return urlUpdates[indexPost];
    } finally {
      lockPost.unlock();
    }
  }

  @Override
  public V load(K key) {
    try {
      byte[] bKey = SolrTools.objectToByte(key);
      if (bKey != null) {
        String sKey = _mapName + ":" + Base64.encodeBytes(bKey);

        JsonObject doc = null;
        for (int i = 0; i < 3; i++) {
          try {
            doc = SolrTools.getDoc(getSolrGetUrl(), connectTimeout, readTimeout, sKey);
            break;
          } catch (IOException ioe) {
            doc = null;
            break;
          } catch (Exception e) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e1) {
            }
          }
        }
        if (doc == null) {
          return null;
        }

        String sValue = doc.getString(SolrTools.F_HZ_DATA);
        byte[] bValue = Base64.decode(sValue);
        return (V) SolrTools.byteToObject(bValue);
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
      byte[] bKey = SolrTools.objectToByte(key);
      if (bKey != null) {
        String sKey = _mapName + ":" + Base64.encodeBytes(bKey);

        JsonObject doc = new JsonObject();
        doc.putObject("delete", (new JsonObject()).putString(SolrTools.F_ID, sKey));

        JsonObject jsonResponse = null;
        for (int i = 0; i < 3; i++) {
          try {
            jsonResponse = SolrTools.delDoc(getSolrUpdateUrl(), connectTimeout, readTimeout, doc);
            if (SolrTools.getStatus(jsonResponse) == 0) {
              break;
            }
          } catch (Exception e) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e1) {
            }
          }
        }
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
      byte[] bKey = SolrTools.objectToByte(key);
      byte[] bValue = SolrTools.objectToByte(value);
      String sKey = _mapName + ":" + Base64.encodeBytes(bKey);
      String sValue = Base64.encodeBytes(bValue);

      JsonObject doc = new JsonObject();
      doc.putString(SolrTools.F_ID, sKey);
      doc.putNumber(SolrTools.F_VERSION, 0); // =0 Don’t care (normal overwrite if exists)
      if (_mapName.startsWith("hz_memcache_")) {
        DateFormat dateFormat = new SimpleDateFormat(SolrTools.LOGDateFormatPattern);
        doc.putString(SolrTools.F_HZ_BIRTHDAY, dateFormat.format(new java.util.Date()));
      }
      doc.putString(SolrTools.F_HZ_DATA, sValue);

      JsonObject jsonResponse = null;
      for (int i = 0; i < 3; i++) {
        try {
          jsonResponse = SolrTools.updateDoc(getSolrUpdateUrl(), connectTimeout, readTimeout, doc);
          if (SolrTools.getStatus(jsonResponse) == 0) {
            break;
          }
        } catch (Exception e) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e1) {
          }
        }
      }

      if (SolrTools.getStatus(jsonResponse) != 0) {
        throw new RuntimeException(jsonResponse.encodePrettily());
      }
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
  public Map<K, V> loadAll(Collection<K> keys) { //@wjw_note: 不知道具体个数,此处必须返回null
    return null;
  }

  @Override
  public Set<K> loadAllKeys() { //@wjw_note: 不知道具体个数,此处必须返回null
    return null;
  }

}
