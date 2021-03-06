package org.hazelcast.server;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.hazelcast.server.persistence.MapSolrStore;
import org.tanukisoftware.wrapper.WrapperManager;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.map.proxy.MapProxyImpl;

@SuppressWarnings({ "rawtypes" })
public class HazelcastServerApp<K, V> implements EntryListener<K, V>, DistributedObjectListener {
  public final String CONF_NAME = "hazelcast.xml"; //配置文件
  public static final String DB_CHARSET = "UTF-8"; //数据库字符集
  public static final String HAZELCAST_INSTANCE_NAME = "HAZELCAST_INSTANCE_NAME";

  private String _idDistributedObjectListener;
  private HazelcastInstance _hazelcastInstance;
  private Map<String, String> _mapEntryListener = new HashMap<String, String>();

  //初始化目录和Log4j
  static {
    try {
      File file = new File(System.getProperty("user.dir", ".") + "/conf/");
      if (!file.exists() && !file.mkdirs()) {
        throw new IOException("Can not create:" + System.getProperty("user.dir", ".") + "/conf/");
      }

      file = new File(System.getProperty("user.dir", ".") + "/log/");
      if (!file.exists() && !file.mkdirs()) {
        throw new IOException("Can not create:" + System.getProperty("user.dir", ".") + "/log/");
      }

      file = new File(System.getProperty("user.dir", ".") + "/db/");
      if (!file.exists() && !file.mkdirs()) {
        throw new IOException("Can not create:" + System.getProperty("user.dir", ".") + "/db/");
      }

      String logPath = System.getProperty("user.dir", ".") + "/conf/log4j.xml";
      if (logPath.toLowerCase().endsWith(".xml")) {
        DOMConfigurator.configure(logPath);
      } else {
        PropertyConfigurator.configure(logPath);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void main(String args[]) {
    @SuppressWarnings("unused")
    HazelcastServerApp app = new HazelcastServerApp(args);
  }

  /**
   * 构造函数
   * 
   * @param args
   */
  public HazelcastServerApp(String args[]) {
    if (!doStart()) {
      System.exit(-1);
    }

    java.lang.Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        doStop();
      }
    }));
  }

  public boolean doStart() {
    try {
      if (_hazelcastInstance != null) {
        doStop();
      }

      //new features in Hazelcast 3.0 are XML variables which provided a lot of flexibility for Hazelcast xml based configuration.
      _hazelcastInstance = Hazelcast.newHazelcastInstance(new ClasspathXmlConfig(CONF_NAME, System.getProperties()));
      _idDistributedObjectListener = _hazelcastInstance.addDistributedObjectListener(this);

      if (!WrapperManager.isControlledByNativeWrapper()) {
        System.out.println("Started Standalone HazelcastServer!");
      }

      //从配置文件里判断MapStore的值来预加载是持久化的Queue!
      Map<String, QueueConfig> mapQueueConfig = _hazelcastInstance.getConfig().getQueueConfigs();
      for (String queueName : mapQueueConfig.keySet()) {
        if (queueName.contains("*")) {
          continue;
        }
        QueueConfig qconf = mapQueueConfig.get(queueName);
        if (qconf != null && qconf.getQueueStoreConfig() != null && qconf.getQueueStoreConfig().isEnabled() == true) {
          qconf.getQueueStoreConfig().getProperties().setProperty(HAZELCAST_INSTANCE_NAME, _hazelcastInstance.getName());
          System.out.println("Load Queue from MapStore:" + "q:" + queueName);
          _hazelcastInstance.getQueue(queueName);
        }
      }

      //从配置文件里判断MapStore的值来预加载是持久化的Map!
      Map<String, MapConfig> mapMapConfig = _hazelcastInstance.getConfig().getMapConfigs();
      for (String mapName : mapMapConfig.keySet()) {
        if (mapName.contains("*")) {
          continue;
        }
        MapConfig mconf = mapMapConfig.get(mapName);
        if (mconf.getMapStoreConfig() != null && mconf.getMapStoreConfig() != null && mconf.getMapStoreConfig().isEnabled()) {
          System.out.println("Load Map from MapStore:" + "m:" + mapName);
          _hazelcastInstance.getMap(mapName);
        }
      }

      return true;
    } catch (Exception e) {
      _hazelcastInstance = null;
      e.printStackTrace();
      return false;
    }
  }

  public boolean doStop() {
    try {
      _hazelcastInstance.removeDistributedObjectListener(_idDistributedObjectListener);
      _hazelcastInstance.getLifecycleService().shutdown();

      if (!WrapperManager.isControlledByNativeWrapper()) {
        System.out.println("Stoped Standalone HazelcastServer!");
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    } finally {
      _hazelcastInstance = null;
    }
  }

  @Override
  public void entryAdded(EntryEvent<K, V> event) {
    //System.out.println(event.toString());
  }

  @Override
  public void entryRemoved(EntryEvent<K, V> event) {
    //System.out.println(event.toString());
  }

  @Override
  public void entryUpdated(EntryEvent<K, V> event) {
    //System.out.println(event.toString());
  }

  @Override
  public void entryEvicted(EntryEvent<K, V> event) {
    if (event.getValue() instanceof com.hazelcast.ascii.memcache.MemcacheEntry) {
      _hazelcastInstance.getMap(event.getName()).delete(event.getKey());
    } else {
      try {
        MapProxyImpl imap = (MapProxyImpl) _hazelcastInstance.getMap(event.getName());
        MapStoreWrapper mapStoreWrapper = imap.getService().getMapContainer(event.getName()).getStore();
        if (mapStoreWrapper != null) {
          MapSolrStore mapStore = (MapSolrStore) mapStoreWrapper.getMapStore();
          if (mapStore.isDeleteOnEvict()) {
            mapStoreWrapper.delete(event.getKey());
          }
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  @Override
  public void distributedObjectCreated(DistributedObjectEvent event) {
    try {
      if (event.getDistributedObject() instanceof MapProxyImpl) {
        MapProxyImpl imap = (MapProxyImpl) event.getDistributedObject();

        MapStoreWrapper mapStoreWrapper = imap.getService().getMapContainer(imap.getName()).getStore();
        if (mapStoreWrapper != null && mapStoreWrapper.isEnabled()) {
          String idListener = imap.addEntryListener(this, false);
          _mapEntryListener.put(imap.getName(), idListener);
          System.out.println("addEntryListener for:" + imap.getName());
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void distributedObjectDestroyed(DistributedObjectEvent event) {
    if (event.getDistributedObject() instanceof IMap) {
      IMap imap = (IMap) event.getDistributedObject();

      String idListener = _mapEntryListener.remove(imap.getName());
      if (idListener != null) {
        imap.removeEntryListener(idListener);
      }
    }
  }

}
