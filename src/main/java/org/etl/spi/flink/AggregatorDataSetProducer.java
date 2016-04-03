package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.AggregatorTransformStep;
import org.etl.spi.flink.reducer.aggregator.AggregatorReduceFunction;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Set;

/**
 * .
 */
public class AggregatorDataSetProducer extends AbstractDataSetProducer<AggregatorTransformStep> {
    @Override
    public void validate(AggregatorTransformStep transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput(),transform);
    }

    @Override
    public DataSet<Record> produce(final AggregatorTransformStep transform,DataSetProduceContext dataSetProduceContext) throws Exception {
        if (CollectionUtils.isNotEmpty(transform.getGroupFields())) {
            return  dataSetProduceContext.getDataSet(transform.getInput())
                    .groupBy(new GroupByKeySelector(transform.getGroupFields()))
                    .reduceGroup(new AggregatorReduceFunction(transform));
        }else{
            return  dataSetProduceContext.getDataSet(transform.getInput())
                    .reduceGroup(new AggregatorReduceFunction(transform));
        }
    }


    private static class GroupByKeySelector implements KeySelector<Record, Value> {

        private Set<String> fields;

        public GroupByKeySelector(Set<String> fields) {
            this.fields = fields;
        }

        @Override
        public Value getKey(Record value) throws Exception {
            return new Value(value, fields);
        }
    }



}
