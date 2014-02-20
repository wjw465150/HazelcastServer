package org.hazelcast.server.persistence;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;

import org.wjw.efjson.JsonObject;

import com.hazelcast.core.QueueStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class QueueSolrStore<T> implements QueueStore<T> {
  private final ILogger _logger = Logger.getLogger(QueueSolrStore.class.getName());

  private int connectTimeout = 60 * 1000; //连接超时
  private int readTimeout = 60 * 1000; //读超时

  private String urlGet;
  private String urlUpdate;

  private String _queueName;
  private Properties _properties;

  public QueueSolrStore(Properties properties, String queueName) {
    _properties = properties;
    _queueName = queueName;

    try {
      if (_properties.getProperty("server") == null) {
        throw new RuntimeException("propertie Solr 'server' Can not null");
      }
      if (_properties.getProperty("port") == null) {
        throw new RuntimeException("propertie Solr 'port' Can not null");
      }

      urlGet = "http://" + _properties.getProperty("server") + ":" + _properties.getProperty("port") + "/solr/get?id=";
      urlUpdate = "http://" + _properties.getProperty("server") + ":" + _properties.getProperty("port") + "/solr/update";
      SolrTools.getDoc(urlGet, connectTimeout, readTimeout, "0");

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _queueName + ":init()完成!");
    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public T load(Long key) {
    try {
      byte[] bKey = SolrTools.objectToByte(key);
      if (bKey != null) {
        String sKey = _queueName + ":" + Base64.encodeBytes(bKey);
        JsonObject doc = SolrTools.getDoc(urlGet, connectTimeout, readTimeout, sKey);
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
      if (bKey != null) {
        String sKey = _queueName + ":" + Base64.encodeBytes(bKey);

        JsonObject doc = new JsonObject();
        doc.putObject("delete", (new JsonObject()).putString(SolrTools.F_ID, sKey));
        SolrTools.delDoc(urlUpdate, connectTimeout, readTimeout, doc);
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

      JsonObject jsonResponse = SolrTools.updateDoc(urlUpdate, connectTimeout, readTimeout, doc);
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
