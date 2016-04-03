package org.etl.spi.flink;

import com.google.common.base.Preconditions;
import org.etl.api.Record;
import org.apache.flink.api.common.operators.Order;
import scala.Serializable;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class Value  implements Comparable<Value>, Serializable {

    private List<Comparable> val = new ArrayList<>();

    private Order[] orderings ;

    public Value(){}

    public Value(Record record, String... fields) {
        for (String field : fields) {
            this.addValue(record.getValue(field));
        }
    }

    public Value(Record record, Iterable<String> fields) {
        for (String field : fields) {
            this.addValue(record.getValue(field));
        }
    }


    public Value(Record record,Order[] orders, String... fields) {
        this(orders, fields);
        for (String field : fields) {
            this.addValue(record.getValue(field));
        }
    }
    public Value(Comparable... values ) {

        for (Comparable value : values) {
            this.addValue(value);
        }
    }

    public Value(Order... orderings) {
        this.orderings = orderings;

    }
    public Value(Order[] orderings,Comparable... values ) {
        Preconditions.checkNotNull(orderings);
        Preconditions.checkNotNull(values);
        Preconditions.checkArgument(orderings.length == values.length, MessageFormat.format("the size of {0} is not equal with the length of {1}", orderings, values));
        this.orderings = orderings;
        if (values != null) {

            for (Comparable value : values) {
                this.addValue(value);
            }
        }
    }
    public Value(Record record, Set<String> fields) {
        for (String field : fields) {
            this.addValue(record.getValue(field));
        }
    }

    public void addValue(Comparable value) {
        val.add(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Value value = (Value) o;
        return Objects.equals(val, value.val);
    }

    @Override
    public int hashCode() {
        return Objects.hash(val);
    }


    @Override
    public int compareTo(Value o) {

        int len1 = val.size();
        int len2 = o.val.size();
        int lim = Math.min(len1, len2);

        int k = 0;
        while (k < lim) {
            Comparable c1 = val.get(k);
            Comparable c2 = o.val.get(k);
            if (c1.compareTo(c2)!=0) {
                if (orderings != null) {
                    Order ordering = orderings[k];
                    if (ordering.equals(Order.DESCENDING)) {
                        return c2.compareTo(c1);
                    }
                }
                return c1.compareTo(c2);
            }
            k++;
        }
        return len1-len2;
    }


}