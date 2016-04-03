package org.etl.util;

import java.util.Map;

/**
 * .
 */
public interface ExpressionEngine {

    <T> T evaluate(String expression, Map<String, Object> bindings);
}
