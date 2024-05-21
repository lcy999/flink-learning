package com.bigdata.flink.reporter.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HttpClientUtil, phoenix in 2019/08/14 use HttpClientPool
 *
 * @author Jwxa
 * @version 1.0
 * @date 2019/5/24
 */
public class HttpClientUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HttpClientUtil.class);

  private static PoolingHttpClientConnectionManager poolConnManager;
  private static CloseableHttpClient httpClient;
  private static RequestConfig requestConfig;

  private static int sockTimeout = 120000;
  private static int connectionTimeout = 120000;
  private static int soTimeout = 120000;
  private static int executionCountDefault = 3;
  private static List<String> RST_MSG = new ArrayList<>();

  public static final String CONTENT_TYPE = "Content-Type";
  public static final String ACCEPT = "Accept";
  public static final String HEADER_TOKEN = "Token";
  public static final String USER_COOKIE_KEY = "userId";
  public static final String HEADER_ENCODE = "Authorization";
  public static final String TRACE_ID = "traceId";
  public static final String AUTHORIZATION = "Authorization";

  static {
    try {
      LOG.info("init HttpClientTest start");
      RST_MSG.add("Connection reset");
      SSLContextBuilder builder = new SSLContextBuilder();
      builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
      // support HTTP and HTPPS
      Registry<ConnectionSocketFactory> socketFactoryRegistry =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register("http", PlainConnectionSocketFactory.getSocketFactory())
              .register("https", sslsf)
              .build();
      // init pool
      poolConnManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
      // set max total
      poolConnManager.setMaxTotal(200);
      // set rote
      poolConnManager.setDefaultMaxPerRoute(10);
      // set request config
      requestConfig =
          RequestConfig.custom()

              .setSocketTimeout(sockTimeout)
              .setConnectTimeout(connectionTimeout)
              .setConnectionRequestTimeout(soTimeout)
              .build();
      // init httpClient
      httpClient = getConnection();
      LOG.info("init HttpClientTest end");
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private HttpClientUtil() {
  }

  /**
   * http post
   *
   * @param url
   * @param postData
   * @param token
   * @return String
   */
  public static String postJson(String url, String postData, String token) {
    String result;
    HttpPost post = null;
    try {
      long startTime = System.currentTimeMillis();
      post = new HttpPost(url);
      post.setConfig(requestConfig);
      post.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      post.setHeader(ACCEPT, ContentType.APPLICATION_JSON.toString());
      if (StringUtils.isNotBlank(token)) {
        post.setHeader(HEADER_TOKEN, token);
      }
      if (StringUtils.isNotBlank(postData)) {
        StringEntity entity = new StringEntity(postData, StandardCharsets.UTF_8);
        post.setEntity(entity);
      }
      result = httpExecute(post);
      LOG.debug("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }

  /**
   * http post
   *
   * @param url
   * @param postData
   * @param cookie
   * @return String
   */
  public static String postJsonWithCookie(String url, String postData, String cookie) {
    String result;
    HttpPost post = null;
    try {
      long startTime = System.currentTimeMillis();
      post = new HttpPost(url);
      post.setConfig(requestConfig);
      post.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      post.setHeader(ACCEPT, ContentType.APPLICATION_JSON.toString());
      if (StringUtils.isNotBlank(cookie)) {
        post.setHeader(USER_COOKIE_KEY, cookie);
      }
      if (StringUtils.isNotBlank(postData)) {
        StringEntity entity = new StringEntity(postData, StandardCharsets.UTF_8);
        post.setEntity(entity);
      }
      result = httpExecute(post);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }

  public static String getJsonWithCookie(String url, String cookie) {
    String result;
    HttpGet get = null;
    try {
      long startTime = System.currentTimeMillis();
      get = new HttpGet(url);
      get.setConfig(requestConfig);
      get.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      get.setHeader(ACCEPT, ContentType.APPLICATION_JSON.toString());
      if (StringUtils.isNotBlank(cookie)) {
        get.setHeader(USER_COOKIE_KEY, cookie);
      }
      result = httpExecute(get);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (get != null) {
        get.releaseConnection();
      }
    }
  }

  private static String httpExecute(HttpUriRequest request) throws IOException {
    String method = request.getMethod();
    String param = "";
    String url = request.getURI().toString();
    if (request instanceof HttpPost) {
      HttpPost post = (HttpPost) request;
      HttpEntity entity = post.getEntity();
      if (null != entity ) {
        InputStream content = entity.getContent();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buff = new byte[1024];
        int len=-1;
        while ((len=content.read(buff)) != -1) {
          outputStream.write(buff,0,len);
        }
        byte[] bytes = outputStream.toByteArray();
        param = new String(bytes, "utf-8");
        outputStream.close();
      }
    }


    LOG.debug("{} request url:{}, postData:{}",method,url,param);
    HttpResponse response = httpClient.execute(request);
    int rspCode = response.getStatusLine().getStatusCode();
    HttpEntity entity = response.getEntity();
    if(entity== null){
      return null;
    }
    String entiry = EntityUtils.toString(entity, StandardCharsets.UTF_8);
    LOG.debug("response url:{}  ,code:{},  entity:{}",url,rspCode,entiry);

    return entiry;
  }

  /**
   * Base64 http
   *
   * @param url
   * @param base64Encode
   * @return String
   */
  public static String getJsonByBase64Encode(String url, String base64Encode) {
    String result;
    HttpGet get = null;
    try {
      long startTime = System.currentTimeMillis();
      get = new HttpGet(url);
      get.setConfig(requestConfig);
      get.setHeader(HEADER_ENCODE, base64Encode);
      result = httpExecute(get);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (get != null) {
        get.releaseConnection();
      }
    }
  }
  public static String getJson(String url, String token, String authorization) {
    return getJson(url, token, authorization, null);
  }
    /**
     * http get
     *
     * @param url
     * @param token
     * @return String
     */
  public static String getJson(String url, String token, String authorization, String traceId) {
    String result;
    HttpGet get = null;
    try {
      long startTime = System.currentTimeMillis();
      get = new HttpGet(url);
      get.setConfig(requestConfig);
      get.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      get.setHeader(ACCEPT, ContentType.APPLICATION_JSON.toString());
      if (StringUtils.isNotBlank(traceId)) {
        get.setHeader(TRACE_ID, traceId);
      }
      if (StringUtils.isNotBlank(token)) {
        get.setHeader(HEADER_TOKEN, token);
      }
      if (StringUtils.isNotBlank(authorization)) {
        get.setHeader(AUTHORIZATION, authorization);
      }

      result = httpExecute(get);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (get != null) {
        get.releaseConnection();
      }
    }
  }

  public static String getJsonFromMetadata(String url, String authorization, String traceId) {
    String result;
    HttpGet get = null;
    try {
      long startTime = System.currentTimeMillis();
      get = new HttpGet(url);
      get.setConfig(requestConfig);
      get.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      get.setHeader(ACCEPT, ContentType.APPLICATION_JSON.toString());
      if (StringUtils.isNotBlank(traceId)) {
        get.setHeader(TRACE_ID, traceId);
      }
      if (StringUtils.isNotBlank(authorization)) {
        get.setHeader(AUTHORIZATION, authorization);
      }

      result = httpExecute(get);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (get != null) {
        get.releaseConnection();
      }
    }
  }

  public static String getJsonFromMetadata(String url, String authorization, String traceId,
                                           Map<String, String> paramMap) {
    String result;
    HttpGet get = null;
    try {
      long startTime = System.currentTimeMillis();
      get = new HttpGet(url);
      get.setConfig(requestConfig);
      get.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      get.setHeader(ACCEPT, ContentType.APPLICATION_JSON.toString());
      if (StringUtils.isNotBlank(traceId)) {
        get.setHeader(TRACE_ID, traceId);
      }
      if (StringUtils.isNotBlank(authorization)) {
        get.setHeader(AUTHORIZATION, authorization);
      }
      List<NameValuePair> params = setHttpParams(paramMap);
      String param = URLEncodedUtils.format(params, "UTF-8");
      get.setURI(URI.create(url + "?" + param));
      result = httpExecute(get);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (get != null) {
        get.releaseConnection();
      }
    }
  }


  /**
   * @param url
   * @param authorization
   * @param traceId
   * @param postData
   * @return
   */
  public static String postJson(String url, String authorization, String traceId, String postData) {
    String result;
    HttpPost post = null;
    try {
      long startTime = System.currentTimeMillis();
      post = new HttpPost(url);
      post.setConfig(requestConfig);
      post.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      post.setHeader(ACCEPT, ContentType.APPLICATION_JSON.toString());
      if (StringUtils.isNotBlank(traceId)) {
        post.setHeader(TRACE_ID, traceId);
      }
      if (StringUtils.isNotBlank(authorization)) {
        post.setHeader(AUTHORIZATION, authorization);
      }

      if (StringUtils.isNotBlank(postData)) {
        StringEntity entity = new StringEntity(postData, StandardCharsets.UTF_8);
        post.setEntity(entity);
      }
      result = httpExecute(post);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }

  public static List<NameValuePair> setHttpParams(Map<String, String> paramMap) {
    ArrayList<NameValuePair> params = new ArrayList<>();
    Set<Map.Entry<String, String>> set = paramMap.entrySet();
    for (Map.Entry<String, String> entry : set) {
      params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
    }
    return params;
  }

  private static HttpRequestRetryHandler getHttpRequestRetryHandler() {
    return (exception, executionCount, context) -> {
      if (executionCount > executionCountDefault) {
        return false;
      }
      if (exception instanceof NoHttpResponseException) { // 没有响应，重试
        return true;
      } else if (exception instanceof SocketException && RST_MSG.stream()
          .allMatch(msg -> exception.getMessage().contains(msg))) { // Socket连接异常，重试
        return true;
      } else if (exception instanceof SSLHandshakeException) { // 本地证书异常
        return false;
      } else if (exception instanceof InterruptedIOException) { // 被中断
        return false;
      } else if (exception instanceof UnknownHostException) { // 找不到服务器
        return false;
      } else if (exception instanceof SSLException) { // SSL异常
        return false;
      }
      HttpClientContext clientContext = HttpClientContext.adapt(context);
      HttpRequest request = clientContext.getRequest();
      // 如果请求是幂等的，则重试
      return !(request instanceof HttpEntityEnclosingRequest);
    };
  }

  private static CloseableHttpClient getConnection() {
    CloseableHttpClient client =
        HttpClients.custom()
            .setConnectionManager(poolConnManager)
            .setDefaultRequestConfig(requestConfig)
            .setRetryHandler(getHttpRequestRetryHandler())
            .build();
    return client;
  }

  /**
   * http post
   *
   * @param url
   * @param token
   * @return String
   */
  public static String postJsonWithParam(String url, Map<String, String> param, String token) {
    String result;
    HttpPost post = null;
    try {
      long startTime = System.currentTimeMillis();
      post = new HttpPost(url);
      post.setConfig(requestConfig);
      if (StringUtils.isNotBlank(token)) {
        post.setHeader(HEADER_TOKEN, token);
      }

      List<NameValuePair> httpParams = new ArrayList<>();
      if(param!=null && param.size()>0){
        for(Map.Entry<String, String> item: param.entrySet()){
          httpParams.add(new BasicNameValuePair(item.getKey(), item.getValue()));
        }
      }
      if(httpParams.size()>0){
        post.setEntity(new UrlEncodedFormEntity(httpParams));
      }

      result = httpExecute(post);
      LOG.info("consume time:{} ms", System.currentTimeMillis() - startTime);
      return result;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }

}
