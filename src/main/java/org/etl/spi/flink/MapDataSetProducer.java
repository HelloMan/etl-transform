package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.MapTransformStep;
import org.etl.api.transform.FieldMapping;
import org.etl.util.ExpressionUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

import java.util.Set;

/**
 * .
 */
public class MapDataSetProducer extends AbstractDataSetProducer<MapTransformStep> {

    @Override
    public void validate(MapTransformStep transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput(),transform);
    }

    @Override
    public DataSet<Record> produce(final MapTransformStep transform,final DataSetProduceContext dataSetProduceContext) throws Exception {
        final DataSet<Record> inputDataSet =  dataSetProduceContext.getDataSet(transform.getInput());
        if (transform.getFieldMappings() == null || transform.getFieldMappings().size() == 0) {
            return inputDataSet;
        }
        return inputDataSet.map(new DataSetMapFunction(transform.getFieldMappings() ));
    }

    private static class DataSetMapFunction implements  MapFunction<Record,Record>{

        private final Set<FieldMapping> fieldMappings;


        public DataSetMapFunction(Set<FieldMapping> fieldMappings) {
            this.fieldMappings = fieldMappings;
        }

        @Override
        public Record map(Record value) throws Exception {
            if (fieldMappings.size() > 0) {
                final Record record = new Record();
                for (FieldMapping fieldMapping : fieldMappings) {
                    Comparable fieldValue = ExpressionUtil.evaluate(fieldMapping.getInputField(), value.asMap());
                    record.setValue(fieldMapping.getOutputField(), fieldValue);
                }

                return record;
            } else {
                return value;
            }
        }
    }
}
