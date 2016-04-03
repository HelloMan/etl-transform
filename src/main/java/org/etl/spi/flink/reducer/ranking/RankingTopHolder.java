package org.etl.spi.flink.reducer.ranking;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import org.etl.api.transform.Ranking;

import java.util.Set;

/**
 *
 */
public class RankingTopHolder {

    private final Integer topN;

    private final Set<Ranking> rankings;

    private final String[] sortFields;

    public RankingTopHolder(Integer topN, Set<Ranking> rankings, String[] sortFields) {
        this.topN = topN;
        this.rankings = rankings;
        this.sortFields = sortFields;
    }

    public Integer getTopN() {
        return topN;
    }

    public Set<Ranking> getRankings() {
        return rankings;
    }

    public String[] getSortFields() {
        return sortFields;
    }

    public  Optional<Ranking> getDenseRanker( ) {
        return FluentIterable.from(getRankings()).firstMatch(new Predicate<Ranking>() {
            @Override
            public boolean apply(Ranking ranking) {
                return Ranking.DENSE_RANK.equals(ranking);
            }
        });
    }

    public Optional<Ranking> getRanker( ) {
        return FluentIterable.from(getRankings()).firstMatch(new Predicate<Ranking>() {
            @Override
            public boolean apply(Ranking ranking) {
                return Ranking.RANK.equals(ranking);
            }
        });
    }
}
