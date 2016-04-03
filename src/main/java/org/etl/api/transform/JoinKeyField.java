package org.etl.api.transform;

import java.io.Serializable;

/**
 *
 */
public interface JoinKeyField extends Serializable {

    /**
     * the join  field name of left side
     * @return
     */
    String getInput1FieldName();

    /**
     * the join  field name of right side
     * @return
     */
    String getInput2FieldName();
}
