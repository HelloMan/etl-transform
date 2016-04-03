package org.etl.api.transform;

import java.io.Serializable;

/**
 * Created by chaojun on 15/12/26.
 */
public  interface TransformStep  extends Serializable{

    /**
     * the id of transform
     * @return
     */
    Long getId();


    /**
     *
     * the name of transform
     * @return
     */
    String getName();

}
