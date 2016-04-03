package org.etl.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.util.Map;


public class JexlExpressionEngineImpl implements ExpressionEngine{


    private final static  JexlEngine jexlEngine = new JexlEngine();

    private final static LoadingCache<String, Expression> jexlExpression =  CacheBuilder
            .newBuilder().build(new CacheLoader<String, Expression>() {
                @Override
                public Expression load(String key) throws Exception {
                    return jexlEngine.createExpression(key);
                }
            });

    @Override
    public <T> T evaluate(String expression, Map<String, Object> bindings) {
        return (T) jexlExpression.getUnchecked(expression).evaluate(new MapContext(bindings));
    }

}
