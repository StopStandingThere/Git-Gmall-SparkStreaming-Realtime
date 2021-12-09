package com.szl.gmallpublisher.service.impl;

import com.szl.constants.GmallConstants;
import com.szl.gmallpublisher.bean.Option;
import com.szl.gmallpublisher.bean.Stat;
import com.szl.gmallpublisher.mapper.DauMapper;
import com.szl.gmallpublisher.mapper.OrderMapper;
import com.szl.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourTotal(String date) {
        //获取原始map集合的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建存放新数据的Map集合
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("LH"),(Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvHourTotal(String date) {
        //获取交易额原始Map数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //创建存放新数据的map集合
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //过滤匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage-1)*size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(GmallConstants.ES_QUERY_INDEXNAME)
                .addType("_doc")
                .build();

        SearchResult searchResult = jestClient.execute(search);

        //TODO 获取命中条数
        Long total = searchResult.getTotal();

        //TODO 获取明细数据
        //创建List集合用来存放明细数据
        ArrayList<Map> details = new ArrayList<>();

        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        MetricAggregation aggregations = searchResult.getAggregations();
        //TODO 获取性别聚合组数据
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");

        List<TermsAggregation.Entry> buckets = groupby_user_gender.getBuckets();

        //男生总数
        Long maleCount = 0L;
        for (TermsAggregation.Entry bucket : buckets) {
            if ("M".equals(bucket.getKey())){
                maleCount += bucket.getCount();
            }
        }
        //男生占比
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;

        //女生占比
        double femaleRatio = Math.round((100 - maleRatio) * 10D) / 10D;

        Option maleOption = new Option("男", maleRatio);
        Option femaleOption = new Option("女", femaleRatio);

        //创建存放性别占比的option对象的list
        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOption);
        genderList.add(femaleOption);

        //创建存放性别占比的Stat对象
        Stat genderStat = new Stat("用户性别占比", genderList);

        //TODO 获取年龄聚合组数据
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> ageBuckets = groupby_user_age.getBuckets();

        //20岁以下个数
        Long low20Count = 0L;
        //30岁及30岁以上的个数
        Long up30Count = 0L;
        for (TermsAggregation.Entry ageBucket : ageBuckets) {
            if (Integer.parseInt(ageBucket.getKey())<20){
                low20Count += ageBucket.getCount();
            }
            if (Integer.parseInt(ageBucket.getKey())>=30){
                up30Count += ageBucket.getCount();
            }
        }
        //计算20岁以下年龄占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;

        //计算30岁及30岁以上年龄占比
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        
        //计算20岁到30岁年龄占比
        double up20WithLow30Ratio = Math.round((100 - low20Ratio - up30Ratio) * 10D) / 10D;

        //创建存放30岁以上占比的option对象
        Option up30Option = new Option("30岁及30岁以上", up30Ratio);

        //创建存放20岁以下占比的option对象
        Option low20Option = new Option("20岁以下", low20Ratio);

        //创建存放20岁以下占比的option对象
        Option up20WithLow30Option = new Option("20岁到30岁", up20WithLow30Ratio);

        //创建存放年龄占比的List集合
        ArrayList<Option> ageList = new ArrayList<>();
        ageList.add(low20Option);
        ageList.add(up30Option);
        ageList.add(up20WithLow30Option);

        //创建存放年龄占比的Stat对象
        Stat ageStat = new Stat("用户年龄占比", ageList);

        //创建存放Stat对象的List集合
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(genderStat);
        stats.add(ageStat);

        //创建存放结果数据的Map集合
        HashMap<String, Object> result = new HashMap<>();
        result.put("total",total);
        result.put("stat",stats);
        result.put("detail",details);

        return result;
    }
}
