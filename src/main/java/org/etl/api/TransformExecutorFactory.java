package org.etl.api;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

import java.text.MessageFormat;
import java.util.ServiceLoader;


public final class TransformExecutorFactory {

    public static TransformExecutor getTransformExecutor(final TransformExecutorType type) {

        ServiceLoader<TransformExecutor> transformExecutors = ServiceLoader.load(TransformExecutor.class);

        Optional<TransformExecutor>  transformExecutorOptional = FluentIterable.from(transformExecutors).firstMatch(new Predicate<TransformExecutor>() {
            @Override
            public boolean apply(TransformExecutor input) {
                return input.getExecutorType().equals(type);
            }

        });

        if (transformExecutorOptional.isPresent()) {
            return transformExecutorOptional.get();
        }

        throw new IllegalArgumentException(MessageFormat.format("can not find transform executor for given type {0}", transformExecutorOptional));
    }
}
