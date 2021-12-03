package com.szl.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.szl.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){
        //1.创建list集合用来创建最终结果数据
        ArrayList<Map> result = new ArrayList<>();

        //获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        // 获取交易额总数数据
        Double gmvTotal = publisherService.getGmvTotal(date);

        //2.创建存放新增日活的Map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        //3.创建存放新增设备的map
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id","dau");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        //4.创建存放新增交易额的Map
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id","order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",gmvTotal);

        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSON.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam("id") String id,
                                  @RequestParam("date") String date){

        //1.获取昨天的日期
        String yesterdayDate = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)){
            //2.获取今天的分时数据
            todayMap = publisherService.getDauHourTotal(date);

            //3.获取昨天的分时数据
            yesterdayMap = publisherService.getDauHourTotal(yesterdayDate);

        }else if("order_amount".equals(id)){
            //4.获取今天的分时数据
            todayMap = publisherService.getGmvHourTotal(date);

            //5.获取昨天的分时数据
            yesterdayMap = publisherService.getGmvHourTotal(yesterdayDate);
        }

        //6.创建存放结果数据的Map集合
        HashMap<String, Map> result = new HashMap<>();

        result.put("today",todayMap);
        result.put("yesterday",yesterdayMap);

        return JSON.toJSONString(result);
    }

}
