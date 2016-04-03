package org.etl.api.transform;

import java.io.Serializable;

/**
 *
 */
public interface ProjectionField extends Serializable {
    /**
     * the input field name of projection
     * @return
     */
    String getInputFieldName();

    /**
     * the result field name of projection
     * @return
     */
    String getResultFieldName();

    /**
     * the side of input(input1 or input2)
     * @return
     */
    boolean isFromLeftSide();
}
