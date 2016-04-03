package org.etl.spi.flink.reducer.ranking;

import com.google.common.base.Optional;
import org.etl.api.Record;
import org.etl.api.transform.Ranking;
import org.etl.spi.flink.Value;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.util.Collector;

/**
 *
 */
public final class RankingReduceUtil {

    /**
     * reduce the values and emit the record one by one
     * @param rankingTopHolder
     * @param values
     * @param out
     */
    public static  void rankingReduce(RankingTopHolder rankingTopHolder,Iterable<Record> values, Collector<Record> out) {
        final int count = rankingTopHolder.getTopN() == null ? 0 : rankingTopHolder.getTopN();
        int emitCnt = 0;

        int rowNumber = 1;
        int denseRankRepeatNum = 0;
        Value lastRecordValue = null;
        Record lastRecord = null;

        final boolean hasRanking = CollectionUtils.isNotEmpty(rankingTopHolder.getRankings());

        Optional<Ranking> denseRankOptional = rankingTopHolder.getDenseRanker();

        Optional<Ranking> rankOptional = rankingTopHolder.getRanker();

        for (Record currentRecord : values) {
            Value currentRecordValue = new Value(currentRecord, rankingTopHolder.getSortFields());

            if (hasRanking) {
                for (Ranking ranking : rankingTopHolder.getRankings()) {
                    currentRecord.setValue(ranking.getRanking(), rowNumber);
                }

                if (lastRecordValue != null && denseRankOptional.isPresent()) {
                    if (lastRecordValue.equals(currentRecordValue)) {
                        denseRankRepeatNum++;
                    }
                    currentRecord.setValue(Ranking.DENSE_RANK.getRanking(), rowNumber - denseRankRepeatNum);
                }
                if (lastRecordValue != null && rankOptional.isPresent()) {
                    int currentRankNum = rowNumber;
                    if (lastRecordValue.equals(currentRecordValue)) {
                        currentRankNum = lastRecord.getValue(Ranking.RANK.getRanking());
                    }
                    currentRecord.setValue(Ranking.DENSE_RANK.getRanking(), currentRankNum);
                }

                lastRecord = currentRecord;
                lastRecordValue = currentRecordValue;
                rowNumber++;

            }

            out.collect(currentRecord);

            emitCnt++;

            if (emitCnt == count) {
                break;
            }

        }
    }

}
