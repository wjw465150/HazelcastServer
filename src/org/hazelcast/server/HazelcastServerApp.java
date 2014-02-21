package org.hazelcast.server;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.tanukisoftware.wrapper.WrapperManager;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HazelcastServerApp<K, V> implements EntryListener<K, V>, DistributedObjectListener {
  public final String CONF_NAME = "hazelcast.xml"; //�����ļ�
  public static final String DB_CHARSET = "UTF-8"; //���ݿ��ַ���
  public static final String HAZELCAST_INSTANCE_NAME = "HAZELCAST_INSTANCE_NAME";

  private String _idDistributedObjectListener;
  private HazelcastInstance _hazelcastInstance;
  private Map<String, String> _mapEntryListener = new HashMap<String, String>();

  //��ʼ��Ŀ¼��Log4j
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
   * ���캯��
   * 
   * @param args
   */
  public HazelcastServerApp(String args[]) {
    java.lang.Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        doStop();
      }
    }));

    if (!doStart()) {
      System.exit(-1);
    }
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

      //�������ļ����ж�MapStore��ֵ��Ԥ�����ǳ־û���Queue!
      Map<String, QueueConfig> qMap = _hazelcastInstance.getConfig().getQueueConfigs();
      for (String queueName : qMap.keySet()) {
        QueueConfig qconf = qMap.get(queueName);
        QueueStoreConfig queueStoreConfig = qconf.getQueueStoreConfig();
        if (queueStoreConfig != null && queueStoreConfig.isEnabled() == true) {
          queueStoreConfig.getProperties().setProperty(HAZELCAST_INSTANCE_NAME, _hazelcastInstance.getName());
          System.out.println("Load Queue.Map from MapStore:" + "q:" + queueName);
          _hazelcastInstance.getQueue(queueName);
        }
      }

      //�������ļ����ж�MapStore��ֵ��Ԥ�����ǳ־û���Map!
      Map<String, MapConfig> mMap = _hazelcastInstance.getConfig().getMapConfigs();
      for (String mapName : mMap.keySet()) {
        MapConfig mconf = mMap.get(mapName);
        if (mconf.getMapStoreConfig().isEnabled()) {
          System.out.println("Load Map from MapStore:" + mapName);
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
    }
  }

  @Override
  public void distributedObjectCreated(DistributedObjectEvent event) {
    if (event.getDistributedObject() instanceof IMap) {
      IMap imap = (IMap) event.getDistributedObject();
      String idListener = imap.addEntryListener(this, true);
      _mapEntryListener.put(imap.getName(), idListener);
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
