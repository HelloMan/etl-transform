package org.etl.api.transform;

/**
 * A filter transform is a predicate applied individually to each record.
 * The predicate decides whether to keep the element, or to discard it.
 */
public interface FilterTransformStep extends SingleInputTransformStep {

    String getFilter();

}
