package org.etl.spi.flink.reducer.aggregator;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import org.etl.api.Record;
import org.etl.api.transform.AggregateType;
import org.etl.api.transform.Aggregator;
import org.etl.api.transform.AggregatorTransformStep;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Set;


public class AggregatorReduceFunction extends RichGroupReduceFunction<Record,Record> {

    private final Set<Aggregator> aggregators;

    private final Set<String> groupFields  ;

    public AggregatorReduceFunction(AggregatorTransformStep transform) {
        this.aggregators = transform.getAggregators();
        this.groupFields = transform.getGroupFields();
    }

    @Override
    public void reduce(Iterable<Record> values, Collector<Record> out) throws Exception {
        Record result = null;
        boolean isFirstRecord;
        for (Record currentRecord : values) {
            if (result == null) {
                isFirstRecord = true;
                result = new Record();
                for (String groupField : groupFields) {
                    result.setValue(groupField, currentRecord.getValue(groupField));
                }
            }else{
                isFirstRecord = false;
            }

            for (Aggregator aggregator : aggregators) {
                AggregatorReducer aggregatorReducer = AggregatorReducerFactory.getAggregatorReducer(aggregator.getType());
                if (isFirstRecord) {
                    result = aggregatorReducer.reduceFirst(aggregator, currentRecord,result);
                } else {
                    result = aggregatorReducer.reduceNext(aggregator, currentRecord, result);
                }
            }



        }

        if (result != null) {
            Optional<Aggregator> avgAggregatorOptional =  FluentIterable.from(aggregators).firstMatch(new Predicate<Aggregator>() {
                @Override
                public boolean apply(Aggregator aggregator) {
                    return AggregateType.AVG.equals(aggregator.getType());
                }
            });
            if (avgAggregatorOptional.isPresent()) {
                Aggregator avgAggregator = avgAggregatorOptional.get();
                AvgIntermediateResult avgIntermediateResult = result.getValue(avgAggregator.getResultFieldName());
                result.setValue(avgAggregator.getResultFieldName(), avgIntermediateResult.getSum().divide(BigDecimal.valueOf(avgIntermediateResult.getCount())));
            }
        }
        if (result != null) {
            out.collect(result);
        }

    }


}
