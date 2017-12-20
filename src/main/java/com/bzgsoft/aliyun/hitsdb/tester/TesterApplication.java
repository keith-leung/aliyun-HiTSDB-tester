package com.bzgsoft.aliyun.hitsdb.tester;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@SpringBootApplication
public class TesterApplication {

    public static void main(String[] args) throws InterruptedException, IOException {
        SpringApplication.run(TesterApplication.class, args);

        HiTSDBConfig config = HiTSDBConfig.address(
                "ts-wz9g7eb2l4wmf3h5o.hitsdb.rds.aliyuncs.com",
                3242).config();

        HiTSDB tsdb = HiTSDBClientFactory.connect(config);

        Date dt1 = new Date();
        Calendar car = Calendar.getInstance();
        car.set(2017, 11, 19, 8, 0);
        dt1 = car.getTime();

//        // 构造数据并写入 HiTSDB
//        for (int i = 0; i < 3600; i++) {
//            Point point = Point.metric("test").
//                    tag("V", "1.0").
//                    value(System.currentTimeMillis(), 12.34567).build();
//            Thread.sleep(10);  // 1秒提交1次
//            tsdb.put(point);
//        }

        Date dt2 = new Date();
        car.set(2017, 11, 21, 8, 0);
        dt2 = car.getTime();

        Query query = Query
                .timeRange(dt1.getTime(), dt2.getTime())    // 设置查询时间条件
                .sub(SubQuery.metric("test")
                        .aggregator(Aggregator.SUM)
                        .tag("V", "1.0")
                        .downsample("1h-sum").build())    // 设置子查询
                .build();

        System.out.println("Start time = " + System.currentTimeMillis());
        List<QueryResult> result = tsdb.query(query);
        System.out.println("返回结果：" + result + " end time = " + System.currentTimeMillis());
        // 安全关闭客户端，以防数据丢失。
        System.out.println("关闭");
        tsdb.close();
    }
}
