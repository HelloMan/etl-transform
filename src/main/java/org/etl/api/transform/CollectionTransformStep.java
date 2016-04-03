package org.etl.api.transform;

import org.etl.api.Record;

import java.util.Collection;


public interface CollectionTransformStep extends SingleInputTransformStep {

    Collection<Record> getCollection();
}
