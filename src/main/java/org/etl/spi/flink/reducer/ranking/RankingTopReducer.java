package org.etl.spi.flink.reducer.ranking;

import org.etl.api.Record;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class RankingTopReducer extends RichGroupReduceFunction<Record,Record> {

    private final RankingTopHolder rankingTopHolder;

    public RankingTopReducer(RankingTopHolder rankingTopHolder) {
        this.rankingTopHolder = rankingTopHolder;
    }

    @Override
    public void reduce(Iterable<Record> values, Collector<Record> out) throws Exception {
        RankingReduceUtil.rankingReduce(rankingTopHolder, values, out);
    }

}
