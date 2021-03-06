package org.hazelcast.server.persistence;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

@SuppressWarnings("unchecked")
public class MapSolrStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V>, Runnable {
  private final ILogger _logger = Logger.getLogger(MapSolrStore.class.getName());

  static final String MEMCACHED_PREFIX = "hz_memcache_";
  static final long DAY_30 = 30 * 24 * 60 * 60 * 1000L;

  private String _solrServerUrls;
  private String _coreName = "collection1";
  private int _connectTimeout = 60 * 1000; //连接超时
  private int _readTimeout = 60 * 1000; //读超时

  private JsonArray _stateArray;
  private java.util.List<String> _urlGets;
  private java.util.List<String> _urlUpdates;
  private java.util.List<String> _urlSelects;
  private boolean _deleteOnEvict = false; //是否允许发生Evict时删除持久化里的数据
  private boolean _loadAll = false; //是否在初始化时就加载数据

  private HazelcastInstance _hazelcastInstance;
  private String _mapName;
  private Properties _properties;

  private Lock _lockGet = new ReentrantLock();
  private int _indexGet = -1;
  private Lock _lockPost = new ReentrantLock();
  private int _indexPost = -1;
  private Lock _lockSelect = new ReentrantLock();
  private int _indexSelect = -1;

  private ScheduledExecutorService _scheduleSync = Executors.newSingleThreadScheduledExecutor(); //刷新Solr集群状态的Scheduled

  public MapSolrStore(String mapName, Properties properties) {
    _mapName = mapName;
    _properties = properties;

    try {
      if (_properties.getProperty(SolrTools.SOLR_SERVER_URLS) == null) {
        throw new RuntimeException("propertie Solr '" + SolrTools.SOLR_SERVER_URLS + "' Can not null");
      }
      _solrServerUrls = _properties.getProperty(SolrTools.SOLR_SERVER_URLS);
      if (_properties.getProperty(SolrTools.CORE_NAME) != null) {
        _coreName = _properties.getProperty(SolrTools.CORE_NAME);
      }
      if (_properties.getProperty(SolrTools.CONNECT_TIMEOUT) != null) {
        _connectTimeout = Integer.parseInt(_properties.getProperty(SolrTools.CONNECT_TIMEOUT)) * 1000;
      }
      if (_properties.getProperty(SolrTools.READ_TIMEOUT) != null) {
        _readTimeout = Integer.parseInt(_properties.getProperty(SolrTools.READ_TIMEOUT)) * 1000;
      }
      if (_properties.getProperty(SolrTools.DELETE_ON_EVICT) != null) {
        _deleteOnEvict = Boolean.parseBoolean(_properties.getProperty(SolrTools.DELETE_ON_EVICT));
      }
      if (_properties.getProperty(SolrTools.LOAD_ALL) != null) {
        _loadAll = Boolean.parseBoolean(_properties.getProperty(SolrTools.LOAD_ALL));
      }

      _stateArray = SolrTools.getClusterState(_solrServerUrls, _coreName, _connectTimeout, _readTimeout);
      if (_stateArray == null) {
        throw new RuntimeException("can not connect Solr Cloud:" + "coreName:" + _coreName + ",URLS:" + _solrServerUrls);
      } else {
        _logger.log(Level.INFO, "Solr Cloud Status:" + _stateArray.encodePrettily());
      }

      this._urlGets = new java.util.ArrayList<String>(_stateArray.size());
      this._urlUpdates = new java.util.ArrayList<String>(_stateArray.size());
      this._urlSelects = new java.util.ArrayList<String>(_stateArray.size());
      for (int i = 0; i < _stateArray.size(); i++) {
        JsonObject jNode = _stateArray.<JsonObject> get(i);
        if (jNode.getString("state").equalsIgnoreCase("active") || jNode.getString("state").equalsIgnoreCase("recovering")) {
          this._urlGets.add(jNode.getString("base_url") + "/" + _coreName + "/get?id=");
          this._urlUpdates.add(jNode.getString("base_url") + "/" + _coreName + "/update");
          this._urlSelects.add(jNode.getString("base_url") + "/" + _coreName + "/select");
        }
      }
    } catch (Exception ex) {
      this._urlGets = null;
      this._urlUpdates = null;
      this._urlSelects = null;
      _logger.log(Level.WARNING, ex.getMessage(), ex);
    }
  }

  @Override
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    _hazelcastInstance = hazelcastInstance;
    try {
      solrCommit();
    } catch (Exception ex) {
      _logger.log(Level.WARNING, ex.getMessage(), ex);
    }

    int syncinterval = 30;
    _scheduleSync.scheduleWithFixedDelay(this, 10, syncinterval, TimeUnit.SECONDS);
    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":init()完成!");
  }

  @Override
  public void destroy() {
    _scheduleSync.shutdown();
    _logger.log(Level.INFO, this.getClass().getCanonicalName() + ":" + _mapName + ":destroy()完成!");
  }

  public boolean isDeleteOnEvict() {
    return _deleteOnEvict;
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

  public String getSolrSelectUrl() {
    if (_urlSelects.size() == 1) {
      return _urlSelects.get(0);
    }

    _lockSelect.lock();
    try {
      _indexSelect++;
      if (_indexSelect >= _urlSelects.size()) {
        _indexSelect = 0;
      }

      return _urlSelects.get(_indexSelect);
    } finally {
      _lockSelect.unlock();
    }
  }

  @Override
  //刷新Solr集群状态的Scheduled
  public void run() {
    JsonArray stateArray = SolrTools.getClusterState(_solrServerUrls, _coreName, _connectTimeout, _readTimeout);
    if (stateArray == null) {
      _logger.log(Level.WARNING, "can not connect Solr Cloud:" + "coreName:" + _coreName + ",URLS:" + _solrServerUrls);
      return;
    }
    if (_stateArray.encode().equals(stateArray.encode())) {
      return;
    }
    _stateArray = stateArray;

    java.util.List<String> newUrlGets = new java.util.ArrayList<String>(stateArray.size());
    java.util.List<String> newUrlUpdates = new java.util.ArrayList<String>(stateArray.size());
    java.util.List<String> newUrlSelects = new java.util.ArrayList<String>(stateArray.size());
    for (int i = 0; i < stateArray.size(); i++) {
      JsonObject jj = stateArray.<JsonObject> get(i);
      if (jj.getString("state").equalsIgnoreCase("active") || jj.getString("state").equalsIgnoreCase("recovering")) {
        newUrlGets.add(jj.getString("base_url") + "/" + _coreName + "/get?id=");
        newUrlUpdates.add(jj.getString("base_url") + "/" + _coreName + "/update");
        newUrlSelects.add(jj.getString("base_url") + "/" + _coreName + "/select");
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

    _lockSelect.lock();
    try {
      this._urlSelects.clear();
      this._urlSelects = newUrlSelects;
    } finally {
      _lockSelect.unlock();
    }
  }

  private String buildSolrId(K key) {
    JsonObject jsonKey = new JsonObject();
    if (key instanceof String) {
      jsonKey.putString("C", "S");
      jsonKey.putString("V", (String) key);
    } else {
      jsonKey.putString("C", key.getClass().getName());
      jsonKey.putString("V", JsonObject.toJson(key));
    }
    String id = _mapName + ":" + jsonKey.encode();

    return id;
  }

  private void solrDelete(K key) throws Exception {

    JsonObject doc = new JsonObject();
    doc.putObject("delete", (new JsonObject()).putString(SolrTools.F_ID, buildSolrId(key)));

    JsonObject jsonResponse = null;
    Exception ex = null;
    for (int i = 0; i < _urlUpdates.size(); i++) {
      try {
        jsonResponse = SolrTools.delDoc(getSolrUpdateUrl(), _connectTimeout, _readTimeout, doc);
        if (SolrTools.getStatus(jsonResponse) == 0) {
          ex = null;
          break;
        }
      } catch (Exception e) {
        ex = e;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
        }
      }
    }
    if (ex != null) {
      throw ex;
    }

    if (SolrTools.getStatus(jsonResponse) != 0) {
      throw new RuntimeException(jsonResponse.encodePrettily());
    }
  }

  private V solrGet(K key) throws Exception {
    String id = buildSolrId(key);

    JsonObject doc = null;
    Exception ex = null;
    for (int i = 0; i < _urlGets.size(); i++) {
      try {
        doc = SolrTools.getDoc(getSolrGetUrl(), _connectTimeout, _readTimeout, id);
        ex = null;
        break;
      } catch (Exception e) {
        ex = e;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
    if (doc == null) {
      return null;
    }

    if (_mapName.startsWith(MEMCACHED_PREFIX)) { //判断memcache是否超期
      Date birthday = SolrTools.solrDateFormat.parse(doc.getString(SolrTools.F_HZ_CTIME));
      if ((System.currentTimeMillis() - birthday.getTime()) >= DAY_30) { //超期30天
        solrDelete(key);
        return null;
      }
    }

    String sClass = doc.getString(SolrTools.F_HZ_CLASS);
    String sValue = doc.getString(SolrTools.F_HZ_DATA);
    return (V) JsonObject.fromJson(sValue, Class.forName(sClass));
  }

  private void solrStore(K key, V value) throws Exception {
    JsonObject doc = new JsonObject();
    doc.putString(SolrTools.F_ID, buildSolrId(key));
    doc.putNumber(SolrTools.F_VERSION, 0); // =0 Don’t care (normal overwrite if exists)
    doc.putString(SolrTools.F_HZ_CTIME, SolrTools.solrDateFormat.format(new java.util.Date(System.currentTimeMillis())));

    doc.putString(SolrTools.F_HZ_CLASS, value.getClass().getName());
    doc.putString(SolrTools.F_HZ_DATA, JsonObject.toJson(value));

    JsonObject jsonResponse = null;
    Exception ex = null;
    for (int i = 0; i < _urlUpdates.size(); i++) {
      try {
        jsonResponse = SolrTools.updateDoc(getSolrUpdateUrl(), _connectTimeout, _readTimeout, doc);
        if (SolrTools.getStatus(jsonResponse) == 0) {
          ex = null;
          break;
        }
      } catch (Exception e) {
        ex = e;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
        }
      }
    }
    if (ex != null) {
      throw ex;
    }

    if (SolrTools.getStatus(jsonResponse) != 0) {
      throw new RuntimeException(jsonResponse.encodePrettily());
    }
  }

  private void solrCommit() throws Exception {
    JsonObject jsonResponse = null;
    Exception ex = null;
    for (int i = 0; i < _urlUpdates.size(); i++) {
      try {
        jsonResponse = SolrTools.solrCommit(getSolrUpdateUrl(), _connectTimeout, _readTimeout);
        if (SolrTools.getStatus(jsonResponse) == 0) {
          ex = null;
          break;
        }
      } catch (Exception e) {
        ex = e;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
        }
      }
    }
    if (ex != null) {
      throw ex;
    }

    if (SolrTools.getStatus(jsonResponse) != 0) {
      throw new RuntimeException(jsonResponse.encodePrettily());
    }
  }

  private JsonObject solrSelect(int startIndex, String cursorMark) throws Exception {
    JsonObject solrResponse = null;
    Exception ex = null;
    for (int i = 0; i < _urlSelects.size(); i++) {
      try {
        solrResponse = SolrTools.selectDocs(getSolrSelectUrl(), _connectTimeout, _readTimeout, "id:" + _mapName + "\\:*", startIndex, SolrTools.PAGE_SIZE, cursorMark);
        ex = null;
        break;
      } catch (Exception e) {
        ex = e;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
        }
      }
    }
    if (ex != null) {
      throw ex;
    }

    return solrResponse;
  }

  @Override
  public V load(K key) {
    try {
      return solrGet(key);
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(K key) {
    try {
      solrDelete(key);
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      throw new RuntimeException(e);
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
      solrStore(key, value);
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      throw new RuntimeException(e);
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
    Map<K, V> result = new HashMap<K, V>();
    for (K key : keys) {
      V value = load(key);
      if (value != null) {
        result.put(key, value);
      }
    }
    return result;
  }

  @Override
  public Set<K> loadAllKeys() {
    if (_loadAll == false) {
      return null;
    }

    if (_hazelcastInstance.getCluster().getMembers().size() > 1) {
      return null;
    }

    Set<K> set = new HashSet<K>();
    try {
      boolean stop = false;
      int startIndex = 0;
      int prfexPos = (_mapName + ":").length();
      JsonObject solrResponse = null;
      while (!stop) {
        if (startIndex == 0) {
          solrResponse = solrSelect(startIndex, "*");
        } else {
          solrResponse = solrSelect(startIndex, solrResponse.getString("nextCursorMark"));
        }
        JsonArray docs = solrResponse.getObject("response").getArray("docs");
        if (docs.size() == 0) {
          break;
        }

        JsonObject doc;
        K key;
        for (int i = 0; i < docs.size(); i++) {
          doc = docs.get(i);
          JsonObject jsonKey = new JsonObject(doc.getString(SolrTools.F_ID).substring(prfexPos));
          if (jsonKey.getString("C").equalsIgnoreCase("S")) {
            key = (K) jsonKey.getString("V");
          } else {
            key = (K) JsonObject.fromJson(jsonKey.getString("V"), Class.forName(jsonKey.getString("C")));
          }

          if (_mapName.startsWith(MEMCACHED_PREFIX)) { //判断memcache是否超期
            Date birthday = SolrTools.solrDateFormat.parse(doc.getString(SolrTools.F_HZ_CTIME));
            if ((System.currentTimeMillis() - birthday.getTime()) >= DAY_30) { //超期30天
              solrDelete(key);
              continue;
            }
          }

          set.add(key);
        }
        _logger.log(Level.INFO, "loadAllKeys():" + _mapName + ":size:" + set.size() + ":startIndex:" + startIndex);

        startIndex = startIndex + docs.size();
        int numFound = solrResponse.getObject("response").getInteger("numFound");
        if (startIndex >= numFound) {
          break;
        }
      }
      if (set.size() == 0) {
        return null;
      } else {
        _logger.log(Level.INFO, "Final loadAllKeys():" + _mapName + ":size:" + set.size() + ":startIndex:" + startIndex);
        return set;
      }
    } catch (Exception e) {
      _logger.log(Level.SEVERE, e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

}
