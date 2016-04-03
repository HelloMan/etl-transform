package org.etl.spi.flink;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.etl.api.Record;
import org.etl.api.transform.JoinKeyField;
import org.etl.api.transform.JoinTransformStep;
import org.etl.api.transform.ProjectionField;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * .
 */
public class JoinDataSetProducer extends AbstractDataSetProducer<JoinTransformStep> {
    @Override
    public void validate(JoinTransformStep transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput1(), transform);
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput2(),transform);
    }

    @Override
    public DataSet<Record> produce(final JoinTransformStep transform,DataSetProduceContext dataSetProduceContext) throws Exception {
        DataSet<Record> inputDataset1 = dataSetProduceContext.getDataSet(transform.getInput1());

        DataSet<Record> inputDataset2 = dataSetProduceContext.getDataSet(transform.getInput2());

        JoinOperatorSetsBase<Record,Record> joinOperatorSetsBase = inputDataset1.leftOuterJoin(inputDataset2);

        final String[] input1KeyFields = FluentIterable.from(transform.getJoinKeyFields()).transform(new Function<JoinKeyField, String>() {
            @Nullable
            @Override
            public String apply(JoinKeyField input) {
                return input.getInput1FieldName();
            }
        }).toArray(String.class);

        final String[] input2KeyFields = FluentIterable.from(transform.getJoinKeyFields()).transform(new Function<JoinKeyField, String>() {
            @Nullable
            @Override
            public String apply(JoinKeyField input) {
                return input.getInput2FieldName();
            }
        }).toArray(String.class);

        return joinOperatorSetsBase.where(new JoinKeySelector(input1KeyFields)).equalTo(new JoinKeySelector(input2KeyFields))
                .with(new DataSetJoinFunction(transform.getProjectionFields()));
    }

    private static class DataSetJoinFunction implements JoinFunction<Record, Record, Record> {


        private final Set<ProjectionField> projectionFields;

        public DataSetJoinFunction(Set<ProjectionField> projectionFields) {
            this.projectionFields = projectionFields;
        }

        @Override
        public Record join(Record first, Record second) throws Exception {
            Record record = new Record();
            for (ProjectionField projectionField :projectionFields) {
                String resultFieldName = StringUtils.isEmpty(projectionField.getResultFieldName()) ?
                        projectionField.getInputFieldName() : projectionField.getResultFieldName();
                record.setValue(resultFieldName, projectionField.isFromLeftSide() ?
                        first.getValue(projectionField.getInputFieldName())
                        : second.getValue(projectionField.getInputFieldName()));
            }

            return record;

        }
    }


    private static class JoinKeySelector implements  KeySelector<Record,Value>{

        private final String[] keyFields;

        public JoinKeySelector(String[] keyFields) {
            this.keyFields = keyFields;
        }

        @Override
        public Value getKey(Record value) throws Exception {
            return new Value(value,keyFields);
        }
    }
}
