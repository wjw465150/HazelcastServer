package org.hazelcast.server.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

import org.wjw.efjson.JsonArray;
import org.wjw.efjson.JsonObject;

public abstract class SolrTools {
  static final String UTF_8 = "UTF-8"; //HTTP�����ַ���
  static final String LOGDateFormatPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  static final SafeSimpleDateFormat solrDateFormat = new SafeSimpleDateFormat(LOGDateFormatPattern);

  static final String LOAD_ALL = "loadAll";
  static final int PAGE_SIZE = 100;
  static final String SOLR_SERVER_URLS = "solrServerUrls";
  static final String CONNECT_TIMEOUT = "connectTimeout";
  static final String READ_TIMEOUT = "readTimeout";

  static final String F_ID = "id";
  static final String F_VERSION = "_version_";

  //@wjw_note: schema.xml��Ҫ���:   
  //<dynamicField name="*_dt"  type="date"    indexed="true"  stored="true"/>
  //<dynamicField name="*_s"  type="string"  indexed="true"  stored="true" />
  static final String F_HZ_CTIME = "HZ_T_dt";
  static final String F_HZ_CLASS = "HZ_C_s";
  static final String F_HZ_DATA = "HZ_V_s";

  private SolrTools() {
    //
  }

  public static String sanitizeFilename(String unsanitized) {
    return unsanitized
        .replaceAll("[\\?\\\\/:|<>\\*]", " ") // filter out ? \ / : | < > *
        .replaceAll("\\s", "_"); // white space as underscores
  }

  @SuppressWarnings("unchecked")
  //���صĸ�ʽ��:[
  //  {"state":"active","base_url":"http://192.168.0.143:8983/solr","core":"collection1","node_name":"192.168.0.143:8983_solr","leader":"true"}
  //  ,{"state":"active","base_url":"http://192.168.0.147:8983/solr","core":"collection1","node_name":"192.168.0.147:8983_solr"}
  //  ,{"state":"active","base_url":"http://192.168.0.118:8983/solr","core":"collection1","node_name":"192.168.0.118:8983_solr"}
  //  ,{"state":"active","base_url":"http://192.168.0.145:8983/solr","core":"collection1","node_name":"192.168.0.145:8983_solr","leader":"true"}
  //  ,{"state":"active","base_url":"http://192.168.0.150:8983/solr","core":"collection1","node_name":"192.168.0.150:8983_solr"}
  //  ,{"state":"active","base_url":"http://192.168.0.190:8983/solr","core":"collection1","node_name":"192.168.0.190:8983_solr"}
  //  ,{"state":"active","base_url":"http://192.168.0.146:8983/solr","core":"collection1","node_name":"192.168.0.146:8983_solr","leader":"true"}
  //  ,{"state":"active","base_url":"http://192.168.0.200:8983/solr","core":"collection1","node_name":"192.168.0.200:8983_solr"}
  //  ,{"state":"active","base_url":"http://192.168.0.58:8983/solr","core":"collection1","node_name":"192.168.0.58:8983_solr"}
  //  ,{"state":"active","base_url":"http://192.168.0.144:8983/solr","core":"collection1","node_name":"192.168.0.144:8983_solr","leader":"true"}
  //  ,{"state":"active","base_url":"http://192.168.0.108:8983/solr","core":"collection1","node_name":"192.168.0.108:8983_solr"}
  //  ,{"state":"active","base_url":"http://192.168.0.191:8983/solr","core":"collection1","node_name":"192.168.0.191:8983_solr"}
  //  ]
  //���󷵻�null
  public static JsonArray getClusterState(String solrServers, int connectTimeout, int readTimeout) {
    String[] urlArray = solrServers.split(",");

    String clusterstate;
    JsonArray result = null;
    for (int i = 0; i < urlArray.length; i++) {
      if (urlArray[i].endsWith("/")) {
        clusterstate = urlArray[i] + "zookeeper?wt=json&detail=true&path=%2Fclusterstate.json&_=" + System.currentTimeMillis();
      } else {
        clusterstate = urlArray[i] + "/zookeeper?wt=json&detail=true&path=%2Fclusterstate.json&_=" + System.currentTimeMillis();
      }

      try {
        String bodyText = doGetProcess(clusterstate, connectTimeout, readTimeout, null, null);

        JsonObject jsonBody = new JsonObject(bodyText);
        String data = jsonBody.getObject("znode").getString("data");
        JsonObject jsonData = new JsonObject(data);

        JsonObject shards = jsonData.getObject("collection1").getObject("shards");
        int size = shards.size();
        result = new JsonArray();
        for (int j = 0; j < size; j++) {
          JsonObject jsonShared = shards.getObject("shard" + (j + 1));
          JsonObject replicas = jsonShared.getObject("replicas");

          Map<String, Object> nodes = replicas.toMap();
          for (Object node : nodes.values()) {
            JsonObject jsonNode = new JsonObject((Map<String, Object>) node);
            result.addObject(jsonNode);
          }
        }

        break;
      } catch (Exception e) {
        result = null;
      }
    }

    return result;
  }

  /**
   * ��Solr���صĶ�����,����״̬��
   * 
   * @param solrResponse
   *          - Solr���ص�״̬��
   */
  public static int getStatus(JsonObject solrResponse) {
    return solrResponse.getObject("responseHeader").getInteger("status").intValue();
  }

  public static JsonObject updateDoc(String urlUpdate, int connectTimeout, int readTimeout, JsonObject doc)
      throws IOException {
    JsonObject solrResponse = new JsonObject(doPostProcess(urlUpdate, connectTimeout, readTimeout, "[" + doc.encode() + "]", null, null));
    return solrResponse;
  }

  public static JsonObject delDoc(String urlUpdate, int connectTimeout, int readTimeout, JsonObject doc)
      throws IOException {
    JsonObject solrResponse = new JsonObject(doPostProcess(urlUpdate, connectTimeout, readTimeout, doc.encode(), null, null));
    return solrResponse;
  }

  public static JsonObject getDoc(String urlGet, int connectTimeout, int readTimeout, String id) throws IOException {
    JsonObject solrResponse = new JsonObject(doGetProcess(urlGet + URLEncoder.encode(id, UTF_8), connectTimeout, readTimeout, null, null));
    return solrResponse.getObject("doc");
  }

  public static JsonArray selectDocs(String urlSelect, int connectTimeout, int readTimeout, String query, int start,
      int pageSize) throws IOException {
    String httpUrl = urlSelect + "?wt=json&q=" + URLEncoder.encode(query, UTF_8) + "&start=" + start + "&rows=" + pageSize;
    JsonObject solrResponse = new JsonObject(doGetProcess(httpUrl, connectTimeout, readTimeout, null, null));
    return solrResponse.getObject("response").getArray("docs");
  }

  public static JsonObject solrCommit(String urlUpdate, int connectTimeout, int readTimeout)
      throws IOException {
    JsonObject doc = new JsonObject("{\"commit\":{\"softCommit\": true}}");
    JsonObject solrResponse = new JsonObject(doPostProcess(urlUpdate, connectTimeout, readTimeout, doc.encode(), null, null));
    return solrResponse;
  }

  /**
   * ����HTTP��GET����,�������ҪBASIC��֤,��user�Լ�pass����Ϊnullֵ
   * 
   * @param urlstr
   *          �����URL
   * @param user
   *          �û���
   * @param pass
   *          ����
   * @return �������ķ�����Ϣ
   * @throws IOException
   */
  private static String doGetProcess(String urlstr, int connectTimeout, int readTimeout, String user, String pass)
      throws IOException {
    URL url = new URL(urlstr);

    HttpURLConnection conn = null;
    BufferedReader reader = null;
    try {
      conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(connectTimeout);
      conn.setReadTimeout(readTimeout);
      conn.setUseCaches(false);
      conn.setDoOutput(false);
      conn.setDoInput(true);
      conn.setRequestProperty("Accept", "*/*");
      if (user != null && pass != null) {
        conn.setRequestProperty("Authorization", "Basic "
            + new String(Base64.encodeBytes((user + ":" + pass).getBytes(UTF_8)))); //��ҪBASIC��֤
      }

      conn.connect();

      if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF_8));
      } else {
        reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), UTF_8));
      }
      String line;
      StringBuilder result = new StringBuilder();

      int i = 0;
      while ((line = reader.readLine()) != null) {
        i++;
        if (i != 1) {
          result.append("\n");
        }
        result.append(line);
      }
      return result.toString();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
        }
      }

      //      if (conn != null) {
      //        try {
      //          conn.disconnect();
      //        } catch (Exception ex) {
      //        }
      //      }
    }
  }

  /**
   * ����HTTP��POST����,�������ҪBASIC��֤,��user�Լ�pass����Ϊnullֵ
   * 
   * @param urlstr
   *          �����URL
   * @param data
   *          ����е���Ϣ����
   * @param user
   *          �û���
   * @param pass
   *          ����
   * @return �������ķ�����Ϣ
   * @throws IOException
   */
  private static String doPostProcess(String urlstr, int connectTimeout, int readTimeout, String data, String user,
      String pass)
      throws IOException {
    URL url = new URL(urlstr);

    HttpURLConnection conn = null;
    BufferedReader reader = null;
    OutputStreamWriter writer = null;
    try {
      conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(connectTimeout);
      conn.setReadTimeout(readTimeout);
      conn.setUseCaches(false);
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestProperty("Accept", "*/*");
      conn.setRequestProperty("Content-Type", "application/json;charset=" + UTF_8);
      if (user != null && pass != null) {
        conn.setRequestProperty("Authorization", "Basic "
            + new String(Base64.encodeBytes((user + ":" + pass).getBytes(UTF_8)))); //��ҪBASIC��֤
      }

      conn.connect();

      writer = new OutputStreamWriter(conn.getOutputStream(), UTF_8);
      writer.write(data);
      writer.flush();

      if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF_8));
      } else {
        reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), UTF_8));
      }
      String line;
      StringBuilder result = new StringBuilder();

      int i = 0;
      while ((line = reader.readLine()) != null) {
        i++;
        if (i != 1) {
          result.append("\n");
        }
        result.append(line);
      }
      return result.toString();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
        }
      }

      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ex) {
        }
      }

      //      if (conn != null) {
      //        try {
      //          conn.disconnect();
      //        } catch (Exception ex) {
      //        }
      //      }
    }

  }

  public static class WrapperEntry<K, V> {
    final public java.util.Date _birthday = new java.util.Date();
    public K _key;
    public V _value;

    public WrapperEntry(K key, V value) {
      _key = key;
      _value = value;
    }

  }
}
