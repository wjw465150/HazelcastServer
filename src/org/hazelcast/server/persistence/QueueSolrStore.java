package org.hazelcast.server.persistence;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import org.wjw.efjson.JsonArray;
import org.wjw.efjson.JsonObject;

import com.hazelcast.core.QueueStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class QueueSolrStore<T> implements QueueStore<T>, Runnable {
  private final ILogger _logger = Logger.getLogger(QueueSolrStore.class.getName());

  private String _solrServerUrls;
  private int _connectTimeout = 60 * 1000; //连接超时
  private int _readTimeout = 60 * 1000; //读超时

  private java.util.List<String> _urlGets;
  private java.util.List<String> _urlUpdates;

  private String _queueName;
  private Properties _properties;

  private Lock _lockGet = new ReentrantLock();
  private int _indexGet = -1;
  private Lock _lockPost = new ReentrantLock();
  private int _indexPost = -1;

  private ScheduledExecutorService _scheduleSync = Executors.newSingleThreadScheduledExecutor(); //刷新Solr集群状态的Scheduled

  public QueueSolrStore(Properties properties, String queueName) {
    _properties = properties;
    _queueName = queueName;

    try {
      if (_properties.getProperty(SolrTools.SOLR_SERVER_URLS) == null) {
        throw new RuntimeException("propertie Solr '"+SolrTools.SOLR_SERVER_URLS+"' Can not null");
      }
      _solrServerUrls = _properties.getProperty(SolrTools.SOLR_SERVER_URLS);
      if (_properties.getProperty(SolrTools.CONNECT_TIMEOUT) != null) {
        _connectTimeout = Integer.parseInt(_properties.getProperty(SolrTools.CONNECT_TIMEOUT));
      }
      if (_properties.getProperty(SolrTools.READ_TIMEOUT) != null) {
        _readTimeout = Integer.parseInt(_properties.getProperty(SolrTools.READ_TIMEOUT));
      }

      JsonArray stateArray = SolrTools.getClusterState(_solrServerUrls, _connectTimeout, _readTimeout);
      if (stateArray == null) {
        throw new RuntimeException("can not connect Solr Cloud:" + _solrServerUrls);
      } else {
        _logger.log(Level.INFO, "Solr Cloud Status:" + stateArray.encodePrettily());
      }

      this._urlGets = new java.util.ArrayList<String>(stateArray.size());
      this._urlUpdates = new java.util.ArrayList<String>(stateArray.size());
      for (int i = 0; i < stateArray.size(); i++) {
        JsonObject jNode = stateArray.<JsonObject> get(i);
        if (jNode.getString("state").equalsIgnoreCase("active") || jNode.getString("state").equalsIgnoreCase("recovering")) {
          this._urlGets.add(jNode.getString("base_url") + "/get?id=");
          this._urlUpdates.add(jNode.getString("base_url") + "/update");
        }
      }

      int syncinterval = 10;
      _scheduleSync.scheduleWithFixedDelay(this, 10, syncinterval, TimeUnit.SECONDS);

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":init()完成!");
    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
  }

  public String getSolrGetUrl() {
    if (_urlGets.size() == 1) {
      return _urlGets.get(0);
    }

    _lockGet.lock();
    try {
      _indexGet++;
      if (_indexGet >= _urlGets.size()) {
        _indexGet = 0;
      }

      return _urlGets.get(_indexGet);
    } finally {
      _lockGet.unlock();
    }
  }

  public String getSolrUpdateUrl() {
    if (_urlUpdates.size() == 1) {
      return _urlUpdates.get(0);
    }

    _lockPost.lock();
    try {
      _indexPost++;
      if (_indexPost >= _urlUpdates.size()) {
        _indexPost = 0;
      }

      return _urlUpdates.get(_indexPost);
    } finally {
      _lockPost.unlock();
    }
  }

  @Override
  //刷新Solr集群状态的Scheduled
  public void run() {
    JsonArray stateArray = SolrTools.getClusterState(_solrServerUrls, _connectTimeout, _readTimeout);
    if (stateArray == null) {
      _logger.log(Level.WARNING, "can not connect Solr Cloud:" + _solrServerUrls);
      return;
    }

    java.util.List<String> newUrlGets = new java.util.ArrayList<String>(stateArray.size());
    java.util.List<String> newUrlUpdates = new java.util.ArrayList<String>(stateArray.size());
    for (int i = 0; i < stateArray.size(); i++) {
      JsonObject jj = stateArray.<JsonObject> get(i);
      if (jj.getString("state").equalsIgnoreCase("active") || jj.getString("state").equalsIgnoreCase("recovering")) {
        newUrlGets.add(jj.getString("base_url") + "/get?id=");
        newUrlUpdates.add(jj.getString("base_url") + "/update");
      }
    }

    _lockGet.lock();
    try {
      this._urlGets.clear();
      this._urlGets = newUrlGets;
    } finally {
      _lockGet.unlock();
    }

    _lockPost.lock();
    try {
      this._urlUpdates.clear();
      this._urlUpdates = newUrlUpdates;
    } finally {
      _lockPost.unlock();
    }
  }

  @Override
  public T load(Long key) {
    try {
      byte[] bKey = SolrTools.objectToByte(key);
      if (bKey != null) {
        String sKey = _queueName + ":" + Base64.encodeBytes(bKey);

        JsonObject doc = null;
        for (int i = 0; i < _urlGets.size(); i++) {
          try {
            doc = SolrTools.getDoc(getSolrGetUrl(), _connectTimeout, _readTimeout, sKey);
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
      for (int i = 0; i < _urlUpdates.size(); i++) {
        try {
          jsonResponse = SolrTools.delDoc(getSolrUpdateUrl(), _connectTimeout, _readTimeout, doc);
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
      for (int i = 0; i < _urlUpdates.size(); i++) {
        try {
          jsonResponse = SolrTools.updateDoc(getSolrUpdateUrl(), _connectTimeout, _readTimeout, doc);
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
