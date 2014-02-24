package org.hazelcast.server.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.wjw.efjson.JsonObject;

public abstract class SolrTools {
  static final String UTF_8 = "UTF-8"; //HTTP请求字符集
  static final String LOGDateFormatPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

  static final String F_ID = "id";
  static final String F_VERSION = "_version_";

  //@wjw_note: schema.xml需要添加:   <field name="HZ_DATA" type="text_general" indexed="false" stored="true"/>
  static final String F_HZ_DATA = "HZ_DATA";

  //@wjw_note: schema.xml需要添加:   <field name="HZ_CTIME" type="date" indexed="true" stored="true"/>
  static final String F_HZ_CTIME = "HZ_CTIME";

  private SolrTools() {
    //
  }

  public static Object byteToObject(byte[] bb) throws Exception {
    if (bb == null) {
      return null;
    } else {
      return KryoSerializer.read(bb);
    }
  }

  public static byte[] objectToByte(Object object) throws Exception {
    byte[] bb = KryoSerializer.write(object);

    return bb;
  }

  /**
   * 从Solr返回的对象中,返回状态码
   * 
   * @param solrResponse
   *          - Solr返回的状态码
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

  /**
   * 处理HTTP的GET请求,如果不需要BASIC验证,把user以及pass设置为null值
   * 
   * @param urlstr
   *          请求的URL
   * @param user
   *          用户名
   * @param pass
   *          口令
   * @return 服务器的返回信息
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
            + new String(Base64.encodeBytes((user + ":" + pass).getBytes(UTF_8)))); //需要BASIC验证
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
   * 处理HTTP的POST请求,如果不需要BASIC验证,把user以及pass设置为null值
   * 
   * @param urlstr
   *          请求的URL
   * @param data
   *          入队列的消息内容
   * @param user
   *          用户名
   * @param pass
   *          口令
   * @return 服务器的返回信息
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
            + new String(Base64.encodeBytes((user + ":" + pass).getBytes(UTF_8)))); //需要BASIC验证
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

}
