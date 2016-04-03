package org.etl.spi.flink.reducer.aggregator;

import org.etl.api.Record;
import org.etl.api.transform.Aggregator;

import java.math.BigDecimal;

/**
 *
 */
public class MaxAggregatorReducer implements AggregatorReducer {



    @Override
    public Record reduceFirst(Aggregator aggregator, Record currentRecord, Record result) {
        result.setValue(aggregator.getResultFieldName(), currentRecord.getValue(aggregator.getFieldName()));
        return result;
    }

    @Override
    public Record reduceNext(Aggregator aggregator, Record currentRecord, Record result) {
        BigDecimal currentResult = currentRecord.getValue(aggregator.getFieldName());
        BigDecimal maxResult = result.getValue(aggregator.getResultFieldName());
        if (maxResult == null) {
            maxResult = currentResult;
        }else {
            if (currentResult != null) {
                maxResult = maxResult.max(currentResult);
            }
        }

        result.setValue(aggregator.getResultFieldName(),maxResult);
        return result;
    }
}
