package org.etl.spi.flink.reducer.aggregator;

import org.etl.api.Record;
import org.etl.api.transform.Aggregator;

import java.math.BigDecimal;

/**
 *
 */
public class MinAggregatorReducer implements AggregatorReducer {



    @Override
    public Record reduceFirst(Aggregator aggregator, Record currentRecord, Record result) {
        result.setValue(aggregator.getResultFieldName(), currentRecord.getValue(aggregator.getFieldName()));
        return result;
    }

    @Override
    public Record reduceNext(Aggregator aggregator, Record currentRecord, Record result) {
        BigDecimal currentResult = currentRecord.getValue(aggregator.getFieldName());
        BigDecimal minResult = result.getValue(aggregator.getResultFieldName());
        if (minResult == null) {
            minResult = currentResult;
        }else {
            if (currentResult != null) {
                minResult = minResult.min(currentResult);
            }
        }

        result.setValue(aggregator.getResultFieldName(),minResult);
        return result;
    }
}
