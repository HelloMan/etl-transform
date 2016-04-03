package org.etl.spi.flink.reducer.aggregator;

import com.google.common.collect.ComparisonChain;

import java.math.BigDecimal;

/**
 *
 */
public class AvgIntermediateResult implements Comparable<AvgIntermediateResult> {
    private long count;

    private BigDecimal sum ;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public void setSum(BigDecimal sum) {
        this.sum = sum;
    }

    @Override
    public int compareTo(AvgIntermediateResult o) {
        return ComparisonChain.start().compare(this.count, o.count).compare(this.sum, o.sum).result();
    }
}
