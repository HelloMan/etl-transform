package org.etl;

import com.google.common.collect.Sets;
import org.etl.api.Record;
import org.etl.api.transform.*;

import java.math.BigDecimal;
import java.util.*;

/**
 *
 */
public class TransformStepFactory {
    public static final String MAKER_FIELD = "maker";
    public static final String COLOUR_FIELD = "colour";
    public static final String QUANTITY_FIELD = "quantity";
    public static final String PRICE_FIELD = "price";

    public static final String PRODUCE_AREA_ID_FIELD = "produceAreaId";
    public static final String PRODUCE_AREA_NAME_FIELD = "produceAreaName";

    public static final String[] phonesFields = new String[]{MAKER_FIELD,COLOUR_FIELD,QUANTITY_FIELD,PRICE_FIELD,PRODUCE_AREA_ID_FIELD};

    public static final String[] produceAreaFields = new String[]{PRODUCE_AREA_ID_FIELD,PRODUCE_AREA_NAME_FIELD};


    public static List<Record> createProduceAreaRecords(){
        List<Record> records = new ArrayList<>();
        records.add(createRecord(produceAreaFields,1, "China"));
        records.add(createRecord(produceAreaFields,2, "America"));
        records.add(createRecord(produceAreaFields,3, "England"));

        return records;
    }

    public static List<Record> createPhoneRecords(){
        List<Record> records = new ArrayList<>();
        records.add(createRecord(phonesFields,"HTC", "Red", BigDecimal.valueOf(200), 2500,1));
        records.add(createRecord(phonesFields,"HTC", "Yellow", BigDecimal.valueOf(300), 2400,2));
        records.add(createRecord(phonesFields,"HTC", "White", BigDecimal.valueOf(100), 3500,2));
        records.add(createRecord(phonesFields,"HTC", "Silver", BigDecimal.valueOf(500), 2000,3));

        records.add(createRecord(phonesFields,"HTC", "Red", BigDecimal.valueOf(200), 2500,2));
        records.add(createRecord(phonesFields,"HTC", "White", BigDecimal.valueOf(100), 3500,1));

        records.add(createRecord(phonesFields,"Apple", "Red", BigDecimal.valueOf(300), 3600,1));
        records.add(createRecord(phonesFields,"Apple", "Yellow", BigDecimal.valueOf(100), 2200,2));
        records.add(createRecord(phonesFields,"Apple", "White", BigDecimal.valueOf(250), 5500,3));
        records.add(createRecord(phonesFields,"Apple", "Silver", BigDecimal.valueOf(450), 5028,2));

        records.add(createRecord(phonesFields,"Apple", "White", BigDecimal.valueOf(640), 8800,1));
        records.add(createRecord(phonesFields,"Apple", "Silver", BigDecimal.valueOf(220), 3500,2));
        return records;
    }

    public static Record createRecord(String[] fields,Comparable... values){
        Record record = new Record();
        for (int i = 0; i <= fields.length - 1; i++) {
            record.setValue(fields[i], values[i]);
        }

        return record;

    }

    public static TransformStep createCollectionTransformStep(final Collection<Record> records,final String transformStepName){
        return  new CollectionTransformStep() {
            @Override
            public Collection<Record> getCollection() {
                return records;
            }

            @Override
            public TransformStep getInput() {
                return null;
            }

            @Override
            public Long getId() {
                return 1l;
            }
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransformStep that = (TransformStep) o;

                return !(getName() != null ? !getName().equals(that.getName()) : that.getName() != null);

            }

            @Override
            public int hashCode() {
                return getName() != null ? getName().hashCode() : 0;
            }
            @Override
            public String getName() {
                return transformStepName;
            }

            @Override
            public String toString() {
                return getName();
            }
        };
    }

    public static TransformStep createProduceAreaTransformStep() {
        return createCollectionTransformStep(createProduceAreaRecords(), "dataSource-produceArea");
    }

    public static TransformStep createPhoneRecordTransformStep() {
        return createCollectionTransformStep(createPhoneRecords(), "dataSource-phones");
    }

    public static TransformStep createFilterTransformStep() {
        return  new FilterTransformStep() {


            @Override
            public String getFilter() {
                return "quantity>300";
            }

            @Override
            public TransformStep getInput() {
                return createMapTrasformStep();
            }
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransformStep that = (TransformStep) o;

                return !(getName() != null ? !getName().equals(that.getName()) : that.getName() != null);

            }

            @Override
            public int hashCode() {
                return getName() != null ? getName().hashCode() : 0;
            }
            @Override
            public Long getId() {
                return 2l;
            }

            @Override
            public String getName() {
                return "filterTransformStep";
            }


            @Override
            public String toString() {
                return getName();
            }
        };
    }

    public static FieldMapping createFieldMapping(final String inputField,final String outputField){
        return  new FieldMapping() {
            @Override
            public String getInputField() {
                return inputField;
            }

            @Override
            public String getOutputField() {
                return outputField;
            }
        };
    }

    public static TransformStep createMapTrasformStep() {
        final Set<FieldMapping> fieldMappings = new HashSet<>();
        fieldMappings.add(createFieldMapping("price+200", PRICE_FIELD));
        fieldMappings.add(createFieldMapping(MAKER_FIELD, MAKER_FIELD));
        fieldMappings.add(createFieldMapping(COLOUR_FIELD, COLOUR_FIELD));
        fieldMappings.add(createFieldMapping(QUANTITY_FIELD, QUANTITY_FIELD));
        fieldMappings.add(createFieldMapping(PRODUCE_AREA_ID_FIELD, PRODUCE_AREA_ID_FIELD));
        return  new MapTransformStep() {
            @Override
            public Set<FieldMapping> getFieldMappings() {
                return fieldMappings;
            }

            @Override
            public TransformStep getInput() {
                return createPhoneRecordTransformStep();
            }
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransformStep that = (TransformStep) o;

                return !(getName() != null ? !getName().equals(that.getName()) : that.getName() != null);

            }

            @Override
            public int hashCode() {
                return getName() != null ? getName().hashCode() : 0;
            }
            @Override
            public Long getId() {
                return 3l;
            }

            @Override
            public String getName() {
                return "mapTransformStep";
            }


            @Override
            public String toString() {
                return getName();
            }
        };
    }

    public static TransformStep createDuplicateTransformStep(){
        return new DuplicateTransformStep() {

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransformStep that = (TransformStep) o;

                return !(getName() != null ? !getName().equals(that.getName()) : that.getName() != null);

            }

            @Override
            public int hashCode() {
                return getName() != null ? getName().hashCode() : 0;
            }
            @Override
            public String getCondition() {
                return "price>250";
            }

            @Override
            public TransformStep getInput() {
                return createMapTrasformStep();
            }

            @Override
            public Long getId() {
                return 4l;
            }

            @Override
            public String getName() {
                return "duplicateTransformStep";
            }



            @Override
            public String toString() {
                return getName();
            }
        };
    }

    public static JoinKeyField createJoinKeyField(final String input1FieldName,
                                                                    final String input2FieldName){
        return new JoinKeyField() {
            @Override
            public String getInput1FieldName() {
                return input1FieldName;
            }

            @Override
            public String getInput2FieldName() {
                return input2FieldName;
            }
        };
    }

    public static ProjectionField createProjectionField(final String inputFieldName, final String resultFieldName, final boolean fromLeftSide) {
        return new ProjectionField() {
            @Override
            public String getInputFieldName() {
                return inputFieldName;
            }

            @Override
            public String getResultFieldName() {
                return resultFieldName;
            }

            @Override
            public boolean isFromLeftSide() {
                return fromLeftSide;
            }
        };
    }

    public static TransformStep createJoinTransformStep(){
        return new JoinTransformStep() {
            @Override
            public Set<JoinKeyField> getJoinKeyFields() {
                Set<JoinKeyField> joinKeyFields = new HashSet<>();

                joinKeyFields.add(createJoinKeyField(PRODUCE_AREA_ID_FIELD, PRODUCE_AREA_ID_FIELD));

                return joinKeyFields;
            }

            @Override
            public Set<ProjectionField> getProjectionFields() {
                Set<ProjectionField> projectionFields = new HashSet<>();
                projectionFields.add(createProjectionField(MAKER_FIELD, MAKER_FIELD, true));
                projectionFields.add(createProjectionField(COLOUR_FIELD, COLOUR_FIELD, true));
                projectionFields.add(createProjectionField(QUANTITY_FIELD, QUANTITY_FIELD, true));
                projectionFields.add(createProjectionField(PRICE_FIELD, PRICE_FIELD, true));
                projectionFields.add(createProjectionField(PRODUCE_AREA_ID_FIELD, PRODUCE_AREA_ID_FIELD, true));
                projectionFields.add(createProjectionField(PRODUCE_AREA_NAME_FIELD, PRODUCE_AREA_NAME_FIELD, false));
                return projectionFields;
            }

            @Override
            public TransformStep getInput1() {
                return createPhoneRecordTransformStep();
            }

            @Override
            public TransformStep getInput2() {
                return createProduceAreaTransformStep();
            }

            @Override
            public Long getId() {
                return 58l;
            }

            @Override
            public String getName() {
                return "joinTransformStep";
            }

            @Override
            public String toString() {
                return getName();
            }
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransformStep that = (TransformStep) o;

                return !(getName() != null ? !getName().equals(that.getName()) : that.getName() != null);

            }

            @Override
            public int hashCode() {
                return getName() != null ? getName().hashCode() : 0;
            }

        };
    }

    public static Aggregator createAggregator(final AggregateType aggregateType,
                                              final String fieldName,
                                              final String resultFieldName){
        return new Aggregator() {
            @Override
            public AggregateType getType() {
                return aggregateType;
            }

            @Override
            public String getFieldName() {
                return fieldName;
            }

            @Override
            public String getResultFieldName() {
                return resultFieldName;
            }
        };
    }

    public static TransformStep createAggregateTransformStep(){

        return new AggregatorTransformStep() {
            @Override
            public Set<String> getGroupFields() {
                return Sets.newHashSet(MAKER_FIELD, COLOUR_FIELD);
            }

            @Override
            public Set<Aggregator> getAggregators() {
                Set<Aggregator> result = Sets.newHashSet();
                result.add(createAggregator(AggregateType.Sum, QUANTITY_FIELD, "sumQuantity"));
                result.add(createAggregator(AggregateType.Max, QUANTITY_FIELD, "maxQuantity"));
                result.add(createAggregator(AggregateType.Min, QUANTITY_FIELD, "minQuantity"));
                result.add(createAggregator(AggregateType.AVG, QUANTITY_FIELD, "avgQuantity"));
                return result;
            }

            @Override
            public TransformStep getInput() {
                return createPhoneRecordTransformStep();
            }

            @Override
            public Long getId() {
                return 50l;
            }

            @Override
            public String getName() {
                return "aggregate-transformStep";
            }

            @Override
            public String toString() {
                return getName();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransformStep that = (TransformStep) o;

                return !(getName() != null ? !getName().equals(that.getName()) : that.getName() != null);

            }

            @Override
            public int hashCode() {
                return getName() != null ? getName().hashCode() : 0;
            }
        };
    }
}

