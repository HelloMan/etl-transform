package org.etl.spi.flink.reducer.aggregator;

import org.etl.api.Record;
import org.etl.api.transform.Aggregator;

import java.math.BigDecimal;

/**
 *
 */
public class SumAggregatorReducer implements AggregatorReducer {


    @Override
    public Record reduceFirst(Aggregator aggregator, Record currentRecord, Record result) {
        BigDecimal currentResult = currentRecord.getValue(aggregator.getFieldName());
        if (currentRecord == null) {
            currentResult = BigDecimal.ZERO;
        }
        result.setValue(aggregator.getResultFieldName(),currentResult);
        return result;
    }

    @Override
    public Record reduceNext(Aggregator aggregator, Record currentRecord, Record result) {
        BigDecimal sum = result.getValue(aggregator.getResultFieldName());
        BigDecimal currentValue = currentRecord.getValue(aggregator.getFieldName());
        if (currentValue != null) {
            sum = sum.add(currentValue);
        }
        result.setValue(aggregator.getResultFieldName(), sum);
        return result;
    }
}
