package org.etl.api.transform;

/**
 *
 */
public enum Ranking {

    DENSE_RANK("DenseRank()"),
    RANK("Rank()"),
    ROW_NUMBER("RowNumber()");

    private String ranking;

    Ranking(String ranking) {
        this.ranking = ranking;
    }

    public String getRanking() {
        return ranking;
    }
}
