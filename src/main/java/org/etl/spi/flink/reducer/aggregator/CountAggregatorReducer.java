package org.etl.spi.flink.reducer.aggregator;

import org.etl.api.Record;
import org.etl.api.transform.Aggregator;

/**
 *
 */
public class CountAggregatorReducer implements AggregatorReducer {


    @Override
    public Record reduceFirst(Aggregator aggregator, Record currentRecord, Record result) {
        result.setValue(aggregator.getResultFieldName(), 1l);
        return result;
    }

    @Override
    public Record reduceNext(Aggregator aggregator, Record currentRecord, Record result) {
        long count = result.getValue(aggregator.getResultFieldName());
        result.setValue(aggregator.getResultFieldName(), count++);
        return result;
    }
}
