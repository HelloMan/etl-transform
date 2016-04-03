package org.etl.api.transform;

import java.io.Serializable;


public interface FieldMapping  extends Serializable {

    /**
     * mapping input field or expression script
     * @return
     */
    String getInputField();

    /**
     * mapping output field
     * @return
     */
    String getOutputField();

}
