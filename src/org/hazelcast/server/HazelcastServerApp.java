package org.hazelcast.server;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.tanukisoftware.wrapper.WrapperManager;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastServerApp {
  private final String CONF_NAME = "hazelcast.xml"; //�����ļ�
  private static final String DB_CHARSET = "UTF-8"; //���ݿ��ַ���

  private HazelcastInstance _hazelcastInstance;

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

      _hazelcastInstance = Hazelcast.newHazelcastInstance(new ClasspathXmlConfig(CONF_NAME));

      if (!WrapperManager.isControlledByNativeWrapper()) {
        System.out.println("Started Standalone HazelcastServer!");
      }

      //�������ļ����ж�MapStore��ֵ��Ԥ�����ǳ־û���Queue!
      //@wjw TODO: �ᶪʧ����,��֪�������?
//      Map<String, QueueConfig> qMap = _hazelcastInstance.getConfig().getQConfigs();
//      for (String queueName : qMap.keySet()) {
//        QueueConfig qconf = qMap.get(queueName);
//        String mapName = qconf.getBackingMapRef();
//        if (mapName != null && mapName.length() > 0) {
//          MapConfig mconf = _hazelcastInstance.getConfig().getMapConfig(mapName);
//          if (mconf != null && mconf.getMapStoreConfig().isEnabled()) {
//            System.out.println("Load Queue.Map from MapStore:" + "q:" + queueName);
//            _hazelcastInstance.getQueue(queueName).size();
//          }
//        }
//      }

      //�������ļ����ж�MapStore��ֵ��Ԥ�����ǳ־û���Map!
      Map<String, MapConfig> mMap = _hazelcastInstance.getConfig().getMapConfigs();
      for (String mapName : mMap.keySet()) {
        MapConfig mconf = mMap.get(mapName);
        if (mconf.getMapStoreConfig().isEnabled()) {
          System.out.println("Load Map from MapStore:" + mapName);
          _hazelcastInstance.getMap(mapName).size();
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
}
