package org.hazelcast.server.persistence;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import org.wjw.efjson.JsonObject;

import com.hazelcast.core.QueueStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class QueueSolrStore<T> implements QueueStore<T> {
  private final ILogger _logger = Logger.getLogger(QueueSolrStore.class.getName());

  static final int RETRY_COUNT = 4;

  private int connectTimeout = 60 * 1000; //连接超时
  private int readTimeout = 60 * 1000; //读超时

  private String[] urlGets;
  private String[] urlUpdates;

  private String _queueName;
  private Properties _properties;

  private Lock lockGet = new ReentrantLock();
  private int indexGet = -1;
  private Lock lockPost = new ReentrantLock();
  private int indexPost = -1;

  public QueueSolrStore(Properties properties, String queueName) {
    _properties = properties;
    _queueName = queueName;

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

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":init()完成!");
    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
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
  public T load(Long key) {
    try {
      byte[] bKey = SolrTools.objectToByte(key);
      if (bKey != null) {
        String sKey = _queueName + ":" + Base64.encodeBytes(bKey);

        JsonObject doc = null;
        for (int i = 0; i < RETRY_COUNT; i++) {
          try {
            doc = SolrTools.getDoc(getSolrGetUrl(), connectTimeout, readTimeout, sKey);
            if (doc != null) {
              break;
            }
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
        return (T) SolrTools.byteToObject(bValue);
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
      byte[] bKey = SolrTools.objectToByte(key);
      String sKey = _queueName + ":" + Base64.encodeBytes(bKey);
      JsonObject doc = new JsonObject();
      doc.putObject("delete", (new JsonObject()).putString(SolrTools.F_ID, sKey));

      JsonObject jsonResponse = null;
      for (int i = 0; i < RETRY_COUNT; i++) {
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

      if (SolrTools.getStatus(jsonResponse) != 0) {
        throw new RuntimeException(jsonResponse.encodePrettily());
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
      byte[] bKey = SolrTools.objectToByte(key);
      byte[] bValue = SolrTools.objectToByte(value);
      String sKey = _queueName + ":" + Base64.encodeBytes(bKey);
      String sValue = Base64.encodeBytes(bValue);

      JsonObject doc = new JsonObject();
      doc.putString(SolrTools.F_ID, sKey);
      doc.putNumber(SolrTools.F_VERSION, 0); // =0 Don’t care (normal overwrite if exists)
      doc.putString(SolrTools.F_HZ_DATA, sValue);

      JsonObject jsonResponse = null;
      for (int i = 0; i < RETRY_COUNT; i++) {
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
  public void storeAll(Map<Long, T> map) {
    for (Entry<Long, T> entrys : map.entrySet()) {
      this.store(entrys.getKey(), entrys.getValue());
    }
  }

  @Override
  public Map<Long, T> loadAll(Collection<Long> keys) { //@wjw_note:  不知道具体个数,此处必须返回null
    return null;
  }

  @Override
  public Set<Long> loadAllKeys() { //@wjw_note:  不知道具体个数,此处必须返回null
    return null;
  }
}
