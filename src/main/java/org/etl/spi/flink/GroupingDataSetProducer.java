package org.etl.spi.flink;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.etl.api.Record;
import org.etl.api.transform.GroupingTransformStep;
import org.etl.spi.flink.reducer.ranking.RankingTopHolder;
import org.etl.spi.flink.reducer.ranking.RankingTopReducer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.etl.api.transform.Sorter;

import javax.annotation.Nullable;

/**
 * .
 */
public class GroupingDataSetProducer extends AbstractDataSetProducer<GroupingTransformStep> {
    @Override
    public void validate(GroupingTransformStep transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput(),transform);
    }

    @Override
    public DataSet<Record> produce(final GroupingTransformStep transform, DataSetProduceContext dataSetContext) throws Exception {
        DataSet<Record> inputDataSet = dataSetContext.getDataSet(transform.getInput());

        if (CollectionUtils.isNotEmpty(transform.getGroupFields())) {
            if (CollectionUtils.isNotEmpty(transform.getSortFields())) {
                return buildGroupAndSortDataSet(transform, inputDataSet);
            }else {
                return buildGroupDataSet(transform, inputDataSet);
            }
        }else {
            if (CollectionUtils.isNotEmpty(transform.getSortFields())) {
                return buildSortNonGroupDataSet(transform, inputDataSet);
            }else {
                final RankingTopHolder rankingTopHolder = new RankingTopHolder(transform.getTopN(), transform.getRanking(), null);
                return inputDataSet.reduceGroup(new RankingTopReducer(rankingTopHolder));
            }
        }
    }

    private DataSet<Record> buildSortNonGroupDataSet(GroupingTransformStep transform, DataSet<Record> inputDataSet) {
        final Order[] orders = this.getOrders(transform);
        final String[] sortFields = this.getSortFields(transform);
        final RankingTopHolder rankingTopHolder = new RankingTopHolder(transform.getTopN(), transform.getRanking(), sortFields);
        return inputDataSet.map(new MapFunction<Record, Tuple2<Value, Record>>() {
            @Override
            public Tuple2<Value, Record> map(Record value) throws Exception {
                return new Tuple2<>(new Value(value, orders, sortFields), value);
            }
        }).sortPartition(0, Order.ANY).map(new MapFunction<Tuple2<Value, Record>, Record>() {
            @Override
            public Record map(Tuple2<Value, Record> value) throws Exception {
                return value.f1;
            }
        }).reduceGroup(new RankingTopReducer(rankingTopHolder));
    }

    private DataSet<Record> buildGroupDataSet(final GroupingTransformStep transform, DataSet<Record> inputDataSet) {
        final RankingTopHolder rankingTopHolder = new RankingTopHolder(transform.getTopN(), transform.getRanking(), null);
        return inputDataSet.groupBy(new KeySelector<Record, Value>() {
            @Override
            public Value getKey(Record value) throws Exception {
                return new Value(value, transform.getGroupFields());
            }
        }).reduceGroup(new RankingTopReducer(rankingTopHolder));
    }

    private DataSet<Record> buildGroupAndSortDataSet(final GroupingTransformStep transform, DataSet<Record> inputDataSet) {

        final Order[] orders = getOrders(transform);

        final String[] sortFields = getSortFields(transform);
        final RankingTopHolder rankingTopHolder = new RankingTopHolder(transform.getTopN(), transform.getRanking(), sortFields);
        return inputDataSet.groupBy(new KeySelector<Record, Value>() {
            @Override
            public Value getKey(Record value) throws Exception {
                return new Value(value, transform.getGroupFields());
            }
        }).sortGroup(new KeySelector<Record, Value>() {
            @Override
            public Value getKey(Record value) throws Exception {
                return new Value(value, orders, sortFields);
            }
        }, Order.ANY).reduceGroup(new RankingTopReducer(rankingTopHolder));
    }

    private String[] getSortFields(GroupingTransformStep transform) {
        return FluentIterable.from(transform.getSortFields()).transform(new Function<Sorter, String>() {
                @Nullable
                @Override
                public String apply(Sorter sorter) {
                    return sorter.getSortField();
                }
            }).toArray(String.class);
    }

    private Order[] getOrders(GroupingTransformStep transform) {
        return FluentIterable.from(transform.getSortFields()).transform(new Function<Sorter, Order>() {
                @Nullable
                @Override
                public Order apply(Sorter sorter) {
                    return Sorter.Order.ASC.equals(sorter.getOrder()) ? Order.ASCENDING : Order.DESCENDING;
                }
            }).toArray(Order.class);
    }


}
