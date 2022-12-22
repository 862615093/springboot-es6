package com.ww.es.controller;

import com.alibaba.fastjson.JSON;
import com.github.xiaoymin.knife4j.annotations.ApiOperationSupport;
import com.ww.es.base.BaseResult;
import com.ww.es.pojo.Hotel;
import com.ww.es.pojo.HotelDoc;
import com.ww.es.service.IHotelService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("es/")
@Api(tags = "es查询常用API")
public class ESController {

    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @GetMapping("bulkRequest")
    @ApiOperation("添加数据")
    @ApiOperationSupport(order = 1)
    public BaseResult<?> bulkRequest() throws IOException {
        // 查询所有的酒店数据
        List<Hotel> list = hotelService.list();
        // 1.准备Request
        BulkRequest request = new BulkRequest();
        // 2.准备参数
        for (Hotel hotel : list) {
            // 2.1.转为HotelDoc
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 2.2.转json
            String json = JSON.toJSONString(hotelDoc);
            // 2.3.添加请求
            request.add(new IndexRequest("hotel").type("hotel_type").id(hotel.getId().toString()).source(json, XContentType.JSON));
        }
        // 3.发送请求
        client.bulk(request, RequestOptions.DEFAULT);
        return BaseResult.success();
    }

    @GetMapping("matchAll")
    @ApiOperation("查询所有")
    @ApiOperationSupport(order = 2)
    public BaseResult<?> matchAll() throws IOException {
        HashMap<String, Object> hm = new HashMap<>();
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.准备请求参数
        request.source().query(QueryBuilders.matchAllQuery()).size(3);
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        ArrayList<HotelDoc> list = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits();
        hm.put("total", total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            list.add(hotelDoc);
        }
        hm.put("data", list);
        return BaseResult.success(hm);
    }

    @GetMapping("match/multiMatch/copy_to")
    @ApiOperation("全文检索（match/multiMatch/copy_to）")
    @ApiOperationSupport(order = 3)
    public BaseResult<?> match() throws IOException {
        HashMap<String, Object> hm = new HashMap<>();
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.准备请求参数
//        request.source().query(QueryBuilders.matchQuery("all", "如家")).size(3);
        request.source().query(QueryBuilders.multiMatchQuery("外滩如家", "name", "brand", "city")).size(3); // 影响性能
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        ArrayList<HotelDoc> list = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits();
        hm.put("total", total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            list.add(hotelDoc);
        }
        hm.put("data", list);
        return BaseResult.success(hm);
    }

    @GetMapping("term")
    @ApiOperation("精确查询-词条精确匹配")
    @ApiOperationSupport(order = 4)
    public BaseResult<?> term() throws IOException {
        HashMap<String, Object> hm = new HashMap<>();
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.准备请求参数
        request.source().query(QueryBuilders.termQuery("city", "上海")).size(3);
        //term 对 text 类型无效 精确查询一般是查找keyword、数值、日期、boolean等类型字段
//        request.source().query(QueryBuilders.termQuery("name", "7天连锁酒店(北京天坛店)")).size(3);
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        ArrayList<HotelDoc> list = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits();
        hm.put("total", total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            list.add(hotelDoc);
        }
        hm.put("data", list);
        return BaseResult.success(hm);
    }

    @GetMapping("range")
    @ApiOperation("精确查询-范围查询")
    @ApiOperationSupport(order = 5)
    public BaseResult<?> range() throws IOException {
        HashMap<String, Object> hm = new HashMap<>();
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.准备请求参数
        request.source().query(QueryBuilders.rangeQuery("price").gte(100).lte(150)).size(3);
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        ArrayList<HotelDoc> list = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits();
        hm.put("total", total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            list.add(hotelDoc);
        }
        hm.put("data", list);
        return BaseResult.success(hm);
    }

    @GetMapping("bool")
    @ApiOperation("布尔查询（组合查询）")
    @ApiOperationSupport(order = 6)
    public BaseResult<?> bool() throws IOException {
        HashMap<String, Object> hm = new HashMap<>();
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.准备请求参数
        request.source().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("city", "上海"))
                //必须匹配，不参与算分，效率更高
                .filter(QueryBuilders.rangeQuery("price").lte(2500))).size(3);
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        ArrayList<HotelDoc> list = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits();
        hm.put("total", total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            list.add(hotelDoc);
        }
        hm.put("data", list);
        return BaseResult.success(hm);
    }

    @GetMapping("sortAndLimit/{page}/{size}")
    @ApiOperation("排序和基本分页（无法深度分页）")
    @ApiOperationSupport(order = 7)
    public BaseResult<?> sortAndLimit(@PathVariable("page") int page, @PathVariable("size") int size) throws IOException {
        //深度分页直接抛异常
        int from = (page - 1) * size;
        if ((from + size) > 10000) return BaseResult.fail("暂不支持深度分页~");
        HashMap<String, Object> hm = new HashMap<>();
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.准备DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchAllQuery());
        // 2.2.排序 sort
        request.source().sort("price", SortOrder.ASC);
        // 2.3.分页 from、size
        request.source().from((page - 1) * size).size(size);
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        ArrayList<HotelDoc> list = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits();
        hm.put("total", total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            list.add(hotelDoc);
        }
        hm.put("data", list);
        return BaseResult.success(hm);
    }

    @GetMapping("highlight")
    @ApiOperation("高亮查询")
    @ApiOperationSupport(order = 8)
    public BaseResult<?> highlight() throws IOException {
        HashMap<String, Object> hm = new HashMap<>();
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        request.types("hotel_type");
        // 2.准备请求参数
        // 2.1.query
        request.source().query(QueryBuilders.matchQuery("all", "外滩如家")).size(3);
        // 2.2.高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        ArrayList<HotelDoc> list = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits();
        hm.put("total", total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 4.6.处理高亮结果
            // 1)获取高亮map
            Map<String, HighlightField> map = hit.getHighlightFields();
            // 2）根据字段名，获取高亮结果
            HighlightField highlightField = map.get("name");
            // 3）获取高亮结果字符串数组中的第1个元素
            String hName = highlightField.getFragments()[0].toString();
            // 4）把高亮结果放到HotelDoc中
            hotelDoc.setName(hName);
            list.add(hotelDoc);
        }
        hm.put("data", list);
        return BaseResult.success(hm);
    }
}
