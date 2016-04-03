package org.etl.api.transform;

import java.io.Serializable;


public interface Aggregator extends Serializable {

    /**
     * the type of aggregate
     * @return
     */
    AggregateType getType();

    /**
     * the aggregate field name
     * @return
     */
    String getFieldName();

    /**
     * the aggregate result field name
     * @return
     */
    String getResultFieldName();

}
