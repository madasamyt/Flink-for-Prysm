package com.nuskin.prysm.flink.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Aggregated skin health snapshot — the output of {@link
 * com.nuskin.prysm.flink.transform.ScanMetricsAggregator}.
 *
 * <p>Contains the merged dimension scores from all scan streams for a single
 * scan session, plus any extra metadata (skin tone, top concerns, etc.).
 */
public class SkinHealthSnapshot implements Serializable {

    private final String customerId;
    private final String scanId;
    private final long eventTimeMs;

    /** Keyed by dimension name (hydration, pigmentation, texture, wrinkle, radiance, overall) */
    private final Map<String, Double> dimensionScores;

    /** Supplementary string-valued metadata (topConcerns, skinToneLabel, etc.) */
    private final Map<String, String> extras;

    public SkinHealthSnapshot(String customerId,
                               String scanId,
                               long eventTimeMs,
                               Map<String, Double> dimensionScores,
                               Map<String, String> extras) {
        this.customerId      = customerId;
        this.scanId          = scanId;
        this.eventTimeMs     = eventTimeMs;
        this.dimensionScores = Collections.unmodifiableMap(dimensionScores);
        this.extras          = Collections.unmodifiableMap(extras);
    }

    public String getCustomerId()              { return customerId; }
    public String getScanId()                  { return scanId; }
    public long   getEventTimeMs()             { return eventTimeMs; }
    public Map<String, Double> getDimensionScores() { return dimensionScores; }
    public Map<String, String> getExtras()     { return extras; }

    public Double getScore(String dimension) {
        return dimensionScores.get(dimension);
    }

    public String getTopConcerns() {
        return extras.getOrDefault("topConcerns", "");
    }

    public String getSkinToneLabel() {
        return extras.getOrDefault("skinToneLabel", "");
    }

    @Override
    public String toString() {
        return "SkinHealthSnapshot{customerId='" + customerId
                + "', scanId='" + scanId
                + "', scores=" + dimensionScores + "}";
    }
}
