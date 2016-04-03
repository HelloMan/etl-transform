package org.etl.example;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.util.Map;

/**
 * Created by jason zhang on 10/27/2015.
 */
public class ExpressionEngine {

    private static JexlEngine jexlEngine = new JexlEngine();

    private static LoadingCache<String, Expression> jexlExpression =  CacheBuilder
            .newBuilder().build(new CacheLoader<String, Expression>() {
                @Override
                public Expression load(String key) throws Exception {
                    return jexlEngine.createExpression(key);
                }
            });

    public static Object evaluateExpression(final String expression, Map<String, Object> bindings) {
        return jexlExpression.getUnchecked(expression).evaluate(new MapContext(bindings));
    }


}
