package org.etl.spi.flink.reducer.aggregator;

import org.etl.api.transform.AggregateType;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class AggregatorReducerFactory {


    private final static  Map<AggregateType, AggregatorReducer> aggregatorReducerMap  = new HashMap<>();
    static {
        aggregatorReducerMap.put(AggregateType.Max, new MaxAggregatorReducer());
        aggregatorReducerMap.put(AggregateType.Min, new MinAggregatorReducer());
        aggregatorReducerMap.put(AggregateType.Count, new CountAggregatorReducer());
        aggregatorReducerMap.put(AggregateType.AVG, new AvgAggregatorReducer());
        aggregatorReducerMap.put(AggregateType.Sum, new SumAggregatorReducer());

    }
    public static AggregatorReducer getAggregatorReducer(AggregateType aggregateType) {
        return aggregatorReducerMap.get(aggregateType);
    }
}
