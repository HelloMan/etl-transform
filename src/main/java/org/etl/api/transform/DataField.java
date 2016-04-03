package org.etl.api.transform;

import java.io.Serializable;

/**
 */
public interface DataField extends Serializable {

    /**
     * the name of field
     * @return
     */
    String getName();

    /**
     * the type of field
     * @return
     */
    FieldType getType();

    enum FieldType {

        Integer,
        Long,
        Decimal,
        Double,
        Float,
        Date,
        String

    }


}
