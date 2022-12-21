package com.ww.es.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;



/**
 * es 初始化
 *
 * @author weiwang127
 */
@Data
@Slf4j
@Configuration
public class ElasticSearchConfig {
    /**
     * 使用的协议
     */
    private static final String SCHEMA = "http";

    /**
     * 集群地址，多个用,隔开
     */
    private String hosts = "172.30.33.215";

    /**
     * 集群地址，多个用,隔开
     */
    private int port = 9200;

    /**
     * 连接超时时间
     */
    private int connectTimeOut = 50000;

    /**
     * 连接超时时间
     */
    private int socketTimeOut = 50000;

    /**
     * 获取连接的超时时间
     */
    private int connectionRequestTimeOut = 50000;

    /**
     * 最大连接数
     */
    private int maxConnectNum = 50;

    /**
     * 最大路由连接数
     */
    private int maxConnectPerRoute = 50;

    private RestClientBuilder builder;

    @Bean
    @ConditionalOnMissingBean(RestHighLevelClient.class)
    public RestHighLevelClient esClient() {
        ArrayList<HttpHost> hostList = new ArrayList<>();
        String[] hostArr = hosts.split(",");
        for (String host : hostArr) {
            hostList.add(new HttpHost(host, port , SCHEMA));
        }
        builder = RestClient.builder(hostList.toArray(new HttpHost[0]));
        setConnectTimeOutConfig();
        setConnectConfig();
        return new RestHighLevelClient(builder);
    }

    /**
     * httpclient的连接延时配置
     */
    public void setConnectTimeOutConfig() {
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(connectTimeOut);
            requestConfigBuilder.setSocketTimeout(socketTimeOut);
            requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
            return requestConfigBuilder;
        });
    }

    /**
     * httpclient的连接数配置
     */
    public void setConnectConfig() {
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(maxConnectNum);
            httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
            return httpClientBuilder;
        });
    }
}
