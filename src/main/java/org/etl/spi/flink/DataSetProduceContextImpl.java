package org.etl.spi.flink;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.FluentIterable;
import org.etl.api.Record;
import org.etl.util.DirectedAcyclicGraph;
import org.apache.commons.lang3.ClassUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.etl.api.transform.*;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class DataSetProduceContextImpl implements DataSetProduceContext {
    private final static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    private final DirectedAcyclicGraph<TransformStep> transformStepDAG = new DirectedAcyclicGraph<>();

    private final Map<TransformStep, DataSet<Record>> transformMap = new ConcurrentHashMap<>();

    private final static Cache<Class<? extends TransformStep>, DataSetProducer> dataSetProducerCache =   CacheBuilder.newBuilder().softValues().build();
    private final Map<Class<? extends TransformStep>, DataSetProducer> dataSetProducerMap = new HashMap<>();

    public DataSetProduceContextImpl() {
        dataSetProducerMap.put(MapTransformStep.class, new MapDataSetProducer());
        dataSetProducerMap.put(FilterTransformStep.class, new FilterDataSetProducer());
        dataSetProducerMap.put(DistinctTransformStep.class, new DistinctDataSetProducer());
        dataSetProducerMap.put(AggregatorTransformStep.class, new AggregatorDataSetProducer());
        dataSetProducerMap.put(GroupingTransformStep.class, new GroupingDataSetProducer());
        dataSetProducerMap.put(DuplicateTransformStep.class, new DuplicateDataSetProducer());
        dataSetProducerMap.put(JoinTransformStep.class, new JoinDataSetProducer());
        dataSetProducerMap.put(UnionTransformStep.class, new UnionDataSetProducer());
        dataSetProducerMap.put(JdbcDataSource.class, new JdbcDataSetProducer());
        dataSetProducerMap.put(CollectionTransformStep.class, new CollectionDataSetProducer());
    }

    @Override
    public DataSet<Record> getDataSet(TransformStep transformStep) throws Exception {
        DataSet<Record> dataSet = transformMap.get(transformStep);
        if (dataSet == null) {
            DataSetProducer dataSetProducer = getDataSetProducer(transformStep);
            dataSetProducer.validate(transformStep,this);
            dataSet = getDataSetProducer(transformStep).produce(transformStep, this);
            transformMap.put(transformStep, dataSet);
        }
        return dataSet;
    }

    private DataSetProducer getDataSetProducer(final TransformStep transformStep) throws Exception {
        return dataSetProducerCache.get(transformStep.getClass(), new Callable<DataSetProducer>() {
            @Override
            public DataSetProducer call() throws Exception {

                Optional<Class<?>> transformIntf = FluentIterable.from(ClassUtils.getAllInterfaces(transformStep.getClass())).firstMatch(new Predicate<Class<?>>() {
                    @Override
                    public boolean apply(Class<?> input) {
                        return   dataSetProducerMap.containsKey(input);
                    }
                });
                if (transformIntf.isPresent()) {
                    return dataSetProducerMap.get(transformIntf.get());
                }else {
                    throw new IllegalArgumentException(MessageFormat.format("Class '{0}' must  implement at least one of following interfaces\n{1}",
                            transformStep.getClass().getCanonicalName(), Joiner.on(",").join(dataSetProducerMap.keySet())));
                }

            }
        });

    }

    @Override
    public ExecutionEnvironment getExecutionEnvironment() {
        return env;
    }

    @Override
    public DirectedAcyclicGraph<TransformStep> getTransformStepDAG() {
        return transformStepDAG;
    }


}
