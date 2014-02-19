package org.hazelcast.server.persistence;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;

import org.wjw.efjson.JsonObject;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class SolrStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {
  static final String UTF_8 = "UTF-8"; //HTTP�����ַ���

  static final String F_ID = "id";
  static final String F_VERSION = "_version_";
  //@wjw_note: schema.xml��Ҫ���:   <field name="HZ_DATA" type="text_general" indexed="false" stored="true"/>
  static final String F_HZ_DATA = "HZ_DATA";

  private final ILogger _logger = Logger.getLogger(SolrStore.class.getName());

  private int connectTimeout = 60 * 1000; //���ӳ�ʱ
  private int readTimeout = 60 * 1000; //����ʱ

  private String urlGet;
  private String urlUpdate;

  private String _mapName;
  private Properties _properties;

  @Override
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    _properties = properties;
    _mapName = mapName;

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

      _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":init()���!");
    } catch (Exception ex) {
      _logger.log(Level.SEVERE, ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void destroy() {
    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":destroy()���!");
  }

  @Override
  public V load(K key) {
    try {
      byte[] bKey = SolrTools.objectToByte(key);
      if (bKey != null) {
        String sKey = _mapName + ":" + Base64.encodeBytes(bKey);
        JsonObject doc = SolrTools.getDoc(urlGet, connectTimeout, readTimeout, sKey);
        if (doc == null) {
          return null;
        }

        String sValue = doc.getString(F_HZ_DATA);
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
        doc.putObject("delete", (new JsonObject()).putString(F_ID, sKey));
        SolrTools.delDoc(urlUpdate, connectTimeout, readTimeout, doc);
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
      doc.putString(F_ID, sKey);
      doc.putNumber(F_VERSION, 0); // =0 Don��t care (normal overwrite if exists)
      doc.putString(F_HZ_DATA, sValue);

      JsonObject jsonResponse = SolrTools.updateDoc(urlUpdate, connectTimeout, readTimeout, doc);
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
  public Map<K, V> loadAll(Collection<K> keys) { //@wjw_note: ��֪���������,�˴����뷵��null
    return null;
  }

  @Override
  public Set<K> loadAllKeys() { //@wjw_note: ��֪���������,�˴����뷵��null
    return null;
  }

}
