package org.etl.util;

import java.util.Map;

/**
 * .
 */
public class ExpressionUtil {
    private static final ExpressionEngine expressionEngine = new JexlExpressionEngineImpl();

    public static  <T> T evaluate(String expression, Map<String, Object> bindings) {
        return expressionEngine.evaluate(expression, bindings);
    }
}
