package org.etl.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jason zhang on 12/14/2015.
 */
public class DecisionProcessTuple {

    private static MapFunction<String,Tuple3<String,String,Integer>> splitFunc = new MapFunction<String, Tuple3<String, String, Integer>>() {
        public Tuple3<String, String, Integer> map(String value) throws Exception {
            String[] fields = value.split(",");
            return new Tuple3<String, String, Integer>(fields[0], fields[1], Integer.valueOf(fields[2]));
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

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();


        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, String, Integer>> phonesDataSet =   streamEnv.readTextFile("d:\\phones.txt").map(splitFunc);


        DataStream<String> phonesSocket = streamEnv.socketTextStream("172.20.30.211", 9999);
//        phonesSocket.map(splitFunc).union(phonesDataSet).print();

        phonesSocket.map(splitFunc).union(phonesDataSet).flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, BigInteger>>() {
            public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple3<String, String, BigInteger>> out) throws Exception {
                Map<String, Object> sourceRow = new HashMap<String, Object>();
                sourceRow.put("MAKE", value.f0);
                sourceRow.put("COLOUR", value.f1);
                sourceRow.put("QUANTITY", value.f2);

                for (Tuple4<String, String, String, String> dtRow : decisions) {
                    boolean condition = (Boolean) ExpressionEngine.evaluateExpression(dtRow.f0, sourceRow);
                    if (condition) {
                        out.collect(new Tuple3<String, String, BigInteger>(dtRow.f1, dtRow.f2, new BigInteger(sourceRow.get(dtRow.f3).toString())));
                    }
                }
            }
        }).keyBy(0, 1) .reduce(new ReduceFunction<Tuple3<String, String, BigInteger>>() {
            public Tuple3<String, String, BigInteger> reduce(Tuple3<String, String, BigInteger> value1, Tuple3<String, String, BigInteger> value2) throws Exception {
                return new Tuple3<String, String, BigInteger>(value1.f0, value1.f1, value1.f2.add(value2.f2));
            }
        });

        streamEnv.execute();
    }



}
