package com.ww.es.controller;

import com.alibaba.fastjson.JSON;
import com.github.xiaoymin.knife4j.annotations.ApiOperationSupport;
import com.ww.es.base.BaseResult;
import com.ww.es.pojo.HotelDoc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("es/high/")
@Api(tags = "es查询高级API")
public class ESHighController {

    @Autowired
    private RestHighLevelClient client;


    @GetMapping("aggregations/bucket")
    @ApiOperation("聚合查询-桶聚合")
    @ApiOperationSupport(order = 1)
    public BaseResult<?> aggregations() throws IOException {
        // 1.准备请求
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.请求参数
        // 2.1.size , range
        request.source().size(0);
        request.source().query(QueryBuilders.rangeQuery("price").gte(1000));
        // 2.2.聚合 并根据数量排序
        request.source().aggregation(
                AggregationBuilders.terms("cityAgg").order(BucketOrder.count(true)).field("city").size(10));
        // 3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析结果
        Aggregations aggregations = response.getAggregations();
        // 4.1.根据聚合名称，获取聚合结果
        Terms cityAgg = aggregations.get("cityAgg");
        // 4.2.获取buckets
        List<? extends Terms.Bucket> buckets = cityAgg.getBuckets();
        // 4.3.遍历
        ArrayList<Object> list = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            HashMap<String, Object> hm = new HashMap<>();
            String cityName = bucket.getKeyAsString();
            hm.put("cityName", cityName);
            long docCount = bucket.getDocCount();
            hm.put("docCount", docCount);
            list.add(hm);
        }
        return BaseResult.success(list);
    }

    @GetMapping("aggregations/metric")
    @ApiOperation("聚合查询-度量聚合")
    @ApiOperationSupport(order = 2)
    public BaseResult<?> metric() throws IOException {
        // 1.准备请求
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.请求参数
        // 2.1.size , range
        request.source().size(0);
        request.source().query(QueryBuilders.rangeQuery("price").gte(1000));
        // 2.2.聚合 并根据数量排序
        request.source().aggregation(
                AggregationBuilders.terms("brandAgg").order(BucketOrder.count(true)).field("brand").size(20)
                        //度量聚合=子聚合
                        .subAggregation(AggregationBuilders.stats("scoreAgg").field("score")));
        // 3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析结果
        Aggregations aggregations = response.getAggregations();
        // 4.1.根据聚合名称，获取聚合结果
        Terms brandAgg = aggregations.get("brandAgg");
        // 4.2.获取buckets
        List<? extends Terms.Bucket> buckets = brandAgg.getBuckets();
        // 4.3.遍历
        ArrayList<Object> list = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            HashMap<String, Object> hm = new HashMap<>();
            String cityName = bucket.getKeyAsString();
            hm.put("cityName", cityName);
            long docCount = bucket.getDocCount();
            hm.put("docCount", docCount);
            Aggregations subAgg = bucket.getAggregations();
            hm.put("subAgg", subAgg.asMap());
            list.add(hm);
        }
        return BaseResult.success(list);
    }

}
