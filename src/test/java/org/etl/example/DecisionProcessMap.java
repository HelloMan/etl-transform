package org.etl.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jason zhang on 12/14/2015.
 */
public class DecisionProcessMap {

    private static MapFunction<String,Map<String,Object>> splitFunc = new MapFunction<String, Map<String,Object>>() {
        public Map<String,Object> map(String value) throws Exception {
            String[] fields = value.split(",");
            Map<String,Object> record = new HashMap<String, Object>();
            record.put("MAKE",  fields[0]);
            record.put("COLOUR", fields[1]);
            record.put("QUANTITY", Integer.valueOf(fields[2]));
            return record;
        }
    };

    private static List<Tuple4<String, String, String, String>> getDecisions() {
        List<Tuple4<String, String, String, String>> result = new ArrayList<Tuple4<String, String, String, String>>();
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Yellow\" && MAKE == \"HTC\"",",YellowHTC","7","QUANTITY"));
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Yellow\" && MAKE == \"Apple\"",",YellowApple","7","QUANTITY"));
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Red\" && MAKE == \"Apple\"",",RedApple","7","QUANTITY"));
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Yellow\" && MAKE == \"Samsung\"",",YellowSamsung","7","QUANTITY"));
        return result;
    }

    public static void main(String[] args) throws Exception {
        final List<Tuple4<String, String, String, String>> decisions = getDecisions();


        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Map<String,Object>> phonesDataSet =   streamEnv.readTextFile("d:\\phones.txt").map(splitFunc);
        DataStream<String> phonesSocket = streamEnv.socketTextStream("172.20.30.211", 9999);
//        phonesSocket.map(splitFunc).union(phonesDataSet).print();

        phonesSocket.map(splitFunc).union(phonesDataSet).flatMap(new FlatMapFunction<Map<String, Object>, Map<String, Object>>() {
            public void flatMap(Map<String, Object> sourceRow, Collector<Map<String, Object>> out) throws Exception {


                for (Tuple4<String, String, String, String> dtRow : decisions) {
                    boolean condition = (Boolean) ExpressionEngine.evaluateExpression(dtRow.f0, sourceRow);
                    if (condition) {
                        Map<String, Object> record = new HashMap<String, Object>(3);
                        record.put("itemCode", dtRow.f1);
                        record.put("zAxisOrdinate", dtRow.f2);
                        record.put("quantity", Long.valueOf(sourceRow.get(dtRow.f3).toString()));
                        out.collect(record);
                    }
                }
            }
        }).keyBy(new KeySelector<Map<String, Object>, String>() {
            public String getKey(Map<String, Object> value) throws Exception {
                return value.get("itemCode").toString() + value.get("zAxisOrdinate").toString();
            }
        }).reduce(new ReduceFunction<Map<String, Object>>() {
            public Map<String, Object> reduce(Map<String, Object> value1, Map<String, Object> value2) throws Exception {
                Map<String, Object> record = new HashMap<String, Object>(3);
                record.put("itemCode", value1.get("itemCode"));
                record.put("zAxisOrdinate", value1.get("zAxisOrdinate"));
                record.put("quantity", Long.valueOf(value1.get("quantity").toString()) + Long.valueOf(value2.get("quantity").toString()));


                return record;
            }
        }).writeAsText("d:\\a.t");

        streamEnv.execute();
    }



}
