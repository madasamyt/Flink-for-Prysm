package com.nuskin.prysm.flink.transform;

import java.io.Serializable;
import java.util.List;

/**
 * Output type of {@link RecommendationEngine} — a ranked list of product
 * recommendations for a single customer scan event.
 */
public class RecommendationOutput implements Serializable {

    private final String customerId;
    private final String scanId;
    private final String recommendationId;
    private final String source;
    private final long   eventTimeMs;
    private final List<RankedProduct> products;

    public RecommendationOutput(String customerId, String scanId,
                                 String recommendationId, String source,
                                 long eventTimeMs, List<RankedProduct> products) {
        this.customerId        = customerId;
        this.scanId            = scanId;
        this.recommendationId  = recommendationId;
        this.source            = source;
        this.eventTimeMs       = eventTimeMs;
        this.products          = products;
    }

    public String getCustomerId()           { return customerId; }
    public String getScanId()               { return scanId; }
    public String getRecommendationId()     { return recommendationId; }
    public String getSource()               { return source; }
    public long   getEventTimeMs()          { return eventTimeMs; }
    public List<RankedProduct> getProducts(){ return products; }

    /** A single product within the ranked recommendation list */
    public static class RankedProduct implements Serializable {
        private final String       productId;
        private final int          rank;
        private final double       score;
        private final List<String> targetConcerns;

        public RankedProduct(String productId, int rank,
                              double score, List<String> targetConcerns) {
            this.productId      = productId;
            this.rank           = rank;
            this.score          = score;
            this.targetConcerns = targetConcerns;
        }

        public String       getProductId()     { return productId; }
        public int          getRank()          { return rank; }
        public double       getScore()         { return score; }
        public List<String> getTargetConcerns(){ return targetConcerns; }
    }

    @Override
    public String toString() {
        return "RecommendationOutput{customerId='" + customerId
                + "', products=" + products.size() + "}";
    }
}
