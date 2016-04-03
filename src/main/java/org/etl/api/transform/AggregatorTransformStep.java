package org.etl.api.transform;

import java.util.Set;

/**
 * Created by jason zhang on 12/29/2015.
 */
public interface AggregatorTransformStep extends SingleInputTransformStep {
    /**
     * specify the group field
     * @return
     */
    Set<String> getGroupFields();
    /**
     *
     * @return
     */
    Set<Aggregator> getAggregators();

}
