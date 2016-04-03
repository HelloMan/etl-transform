package org.etl.api.transform;

import java.util.Set;


public interface MapTransformStep extends SingleInputTransformStep {



    /**
     * the field mapping for mapping transform
     * @return
     */
    Set<FieldMapping> getFieldMappings();
}
