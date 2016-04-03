package org.etl.spi.flink.reducer.aggregator;

import org.etl.api.Record;
import org.etl.api.transform.Aggregator;

import java.math.BigDecimal;

/**
 *
 */
public class AvgAggregatorReducer implements AggregatorReducer {



    @Override
    public Record reduceFirst(Aggregator aggregator, Record currentRecord, Record result) {
        BigDecimal currentResult = currentRecord.getValue(aggregator.getFieldName());
        AvgIntermediateResult avg = new AvgIntermediateResult();
        avg.setCount(1l);
        if (currentResult == null) {
            currentResult = BigDecimal.ZERO;
        }
        avg.setSum(currentResult);
        result.setValue(aggregator.getResultFieldName(), avg);
        return result;
    }

    @Override
    public Record reduceNext(Aggregator aggregator, Record currentRecord, Record result) {
        AvgIntermediateResult avg = result.getValue(aggregator.getResultFieldName());
        avg.setCount(avg.getCount() + 1);
        BigDecimal currentResult = currentRecord.getValue(aggregator.getFieldName());
        if (currentRecord != null) {
            avg.setSum(avg.getSum().add(currentResult));
        }
        result.setValue(aggregator.getResultFieldName(), avg);
        return result;
    }
}
