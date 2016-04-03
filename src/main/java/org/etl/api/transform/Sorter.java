package org.etl.api.transform;

import java.io.Serializable;


public interface Sorter extends Serializable{

    String getSortField();

    Order getOrder();

    enum Order{
        ASC,DESC
    }
}
