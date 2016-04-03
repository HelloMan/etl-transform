package org.etl.spi.flink.reducer.aggregator;

import org.etl.api.Record;
import org.etl.api.transform.Aggregator;

/**
 *
 */
public interface AggregatorReducer {

    Record reduceFirst(Aggregator aggregator,Record currentRecord, Record result);

    Record reduceNext(Aggregator aggregator,Record currentRecord, Record result);

}
