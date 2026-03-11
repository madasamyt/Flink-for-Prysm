package com.nuskin.prysm.flink.transform;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.model.SkinHealthSnapshot;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Streaming product recommendation engine.
 *
 * <h3>Architecture: Broadcast + Keyed Stream join</h3>
 * <ul>
 *   <li><b>Keyed stream:</b> {@link SkinHealthSnapshot} events, keyed by
 *       {@code customerId}.</li>
 *   <li><b>Broadcast stream:</b> Product catalog events (from
 *       {@code product-catalog} Kinesis stream), broadcast to all task
 *       managers so every recommendation function has a local copy of the
 *       full catalogue.</li>
 * </ul>
 *
 * <h3>Recommendation logic (rule-based + collaborative signals)</h3>
 * <ol>
 *   <li><b>Concern-to-product mapping:</b> Each product declares
 *       {@code targetSkinConcerns} in the catalogue. Products are scored
 *       by how many of the customer's top concerns they address.</li>
 *   <li><b>Severity weighting:</b> Concerns with low scores (e.g. hydration
 *       score &lt; 40) receive a higher weight in the matching.</li>
 *   <li><b>Purchase exclusion:</b> Products the customer already owns
 *       (tracked in per-customer keyed state via purchase events) are
 *       downranked.</li>
 *   <li><b>Skin type compatibility:</b> Products whose {@code targetSkinTypes}
 *       doesn't include the customer's skin type are penalised.</li>
 * </ol>
 *
 * <p>For a production ML-based recommender, replace the scoring logic with
 * a call to a SageMaker endpoint or an ONNX model loaded at open() time.
 */
public class RecommendationEngine
        extends KeyedBroadcastProcessFunction<
                    String,                // key: customerId
                    SkinHealthSnapshot,    // keyed stream
                    GenericRecord,         // broadcast: product catalog
                    RecommendationOutput>  // output
{
    private static final Logger LOG = LoggerFactory.getLogger(RecommendationEngine.class);

    /** Descriptor for the broadcast product catalogue state */
    public static final MapStateDescriptor<String, ProductInfo> CATALOG_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("product-catalog", String.class, ProductInfo.class);

    private final AppConfig appConfig;

    // Per-customer keyed state
    private transient ValueState<String>        skinTypeState;
    private transient ValueState<List<String>>  recentPurchasesState;

    public RecommendationEngine(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @Override
    public void open(Configuration parameters) {
        skinTypeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("skinType", String.class));

        recentPurchasesState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("recentPurchases",
                        Types.LIST(Types.STRING)));
    }

    // ── Keyed stream: SkinHealthSnapshot ────────────────────────────────────

    @Override
    public void processElement(SkinHealthSnapshot snapshot,
                               ReadOnlyContext ctx,
                               Collector<RecommendationOutput> out) throws Exception {
        String customerId = snapshot.getCustomerId();

        ReadOnlyBroadcastState<String, ProductInfo> catalog =
                ctx.getBroadcastState(CATALOG_STATE_DESCRIPTOR);

        List<String> recentPurchases = recentPurchasesState.value();
        if (recentPurchases == null) recentPurchases = new ArrayList<>();

        String skinType = skinTypeState.value();

        // Score each product in the catalogue
        List<ScoredProduct> scored = new ArrayList<>();
        catalog.immutableEntries().forEach(entry -> {
            ProductInfo product = entry.getValue();
            if (!product.isActive()) return;

            double score = scoreProduct(product, snapshot, recentPurchases, skinType);
            scored.add(new ScoredProduct(product.getProductId(), score,
                                         product.getTargetSkinConcerns()));
        });

        if (scored.isEmpty()) {
            LOG.debug("No products in catalogue yet for customerId={}", customerId);
            return;
        }

        // Sort descending by score and take top N
        int topN = appConfig.getRecommendationProductsPerRec();
        scored.sort(Comparator.comparingDouble(ScoredProduct::getScore).reversed());
        List<ScoredProduct> topProducts = scored.subList(0, Math.min(topN, scored.size()));

        RecommendationOutput rec = buildOutput(customerId, snapshot.getScanId(), topProducts);
        out.collect(rec);

        LOG.info("Generated {} recommendations for customerId={}, scanId={}",
                 topProducts.size(), customerId, snapshot.getScanId());
    }

    // ── Broadcast stream: product catalogue updates ──────────────────────────

    @Override
    public void processBroadcastElement(GenericRecord catalogEvent,
                                        Context ctx,
                                        Collector<RecommendationOutput> out) throws Exception {
        BroadcastState<String, ProductInfo> state = ctx.getBroadcastState(CATALOG_STATE_DESCRIPTOR);

        String productId = catalogEvent.get("productId").toString();
        String eventType = catalogEvent.get("eventType").toString();

        if ("DELETED".equals(eventType) || "DISCONTINUED".equals(eventType)) {
            state.remove(productId);
            LOG.info("Removed product {} from broadcast catalogue ({})", productId, eventType);
            return;
        }

        ProductInfo product = ProductInfo.fromGenericRecord(catalogEvent);
        state.put(productId, product);
        LOG.debug("Updated product {} in broadcast catalogue", productId);
    }

    // ── Scoring logic ─────────────────────────────────────────────────────────

    private double scoreProduct(ProductInfo product,
                                SkinHealthSnapshot snapshot,
                                List<String> recentPurchases,
                                String customerSkinType) {
        double score = 0.0;

        // 1. Concern matching — each matched concern contributes weighted by severity
        Map<String, Double> concerns = buildConcernWeights(snapshot);
        for (String concern : product.getTargetSkinConcerns()) {
            Double weight = concerns.get(concern.toLowerCase());
            if (weight != null) {
                score += weight;
            }
        }

        // 2. Skin type compatibility bonus
        if (customerSkinType != null && !product.getTargetSkinTypes().isEmpty()) {
            if (product.getTargetSkinTypes().contains(customerSkinType)) {
                score += 0.2;
            } else {
                score -= 0.1; // Mild penalty for mismatch
            }
        }

        // 3. Recent purchase penalty — avoid recommending already-owned products
        if (recentPurchases.contains(product.getProductId())) {
            score *= 0.3;
        }

        return Math.max(0.0, score);
    }

    /**
     * Converts dimension scores into a map of concern → importance weight.
     * Lower scores on a dimension = higher urgency = higher recommendation weight.
     */
    private Map<String, Double> buildConcernWeights(SkinHealthSnapshot snapshot) {
        Map<String, Double> weights = new HashMap<>();

        addConcernWeight(weights, "hydration",    snapshot.getScore("hydration"),    100.0);
        addConcernWeight(weights, "pigmentation",  snapshot.getScore("pigmentation"), 100.0);
        addConcernWeight(weights, "dark_spots",    snapshot.getScore("pigmentation"), 100.0);
        addConcernWeight(weights, "texture",       snapshot.getScore("texture"),      100.0);
        addConcernWeight(weights, "pores",         snapshot.getScore("poreScore"),    100.0);
        addConcernWeight(weights, "wrinkles",      snapshot.getScore("wrinkle"),      100.0);
        addConcernWeight(weights, "fine_lines",    snapshot.getScore("wrinkle"),      100.0);
        addConcernWeight(weights, "radiance",      snapshot.getScore("radiance"),     100.0);
        addConcernWeight(weights, "clarity",       snapshot.getScore("clarity"),      100.0);

        return weights;
    }

    private void addConcernWeight(Map<String, Double> weights, String concern,
                                  Double score, double maxScore) {
        if (score == null) return;
        // Invert: low score → high weight; normalise to [0, 1]
        double weight = (maxScore - score) / maxScore;
        weights.put(concern, weight);
    }

    private RecommendationOutput buildOutput(String customerId,
                                             String scanId,
                                             List<ScoredProduct> topProducts) {
        List<RecommendationOutput.RankedProduct> ranked = new ArrayList<>();
        for (int i = 0; i < topProducts.size(); i++) {
            ScoredProduct sp = topProducts.get(i);
            ranked.add(new RecommendationOutput.RankedProduct(
                    sp.getProductId(), i + 1, sp.getScore(),
                    sp.getTargetConcerns()));
        }
        return new RecommendationOutput(
                customerId, scanId,
                UUID.randomUUID().toString(),
                "SCAN_RESULTS",
                System.currentTimeMillis(),
                ranked);
    }

    // ── Inner classes ─────────────────────────────────────────────────────────

    /** Lightweight product catalogue entry held in broadcast state */
    public static class ProductInfo implements java.io.Serializable {
        private String productId;
        private List<String> targetSkinConcerns;
        private List<String> targetSkinTypes;
        private boolean active;

        public static ProductInfo fromGenericRecord(GenericRecord rec) {
            ProductInfo p = new ProductInfo();
            p.productId = rec.get("productId").toString();
            p.active    = (boolean) rec.get("isActive");
            Object concerns = rec.get("targetSkinConcerns");
            p.targetSkinConcerns = concerns != null
                    ? Arrays.asList(concerns.toString().replaceAll("[\\[\\]]", "").split(", "))
                    : new ArrayList<>();
            Object types = rec.get("targetSkinTypes");
            p.targetSkinTypes = types != null
                    ? Arrays.asList(types.toString().replaceAll("[\\[\\]]", "").split(", "))
                    : new ArrayList<>();
            return p;
        }

        public String getProductId()               { return productId; }
        public List<String> getTargetSkinConcerns(){ return targetSkinConcerns; }
        public List<String> getTargetSkinTypes()   { return targetSkinTypes; }
        public boolean isActive()                  { return active; }
    }

    private static class ScoredProduct {
        private final String productId;
        private final double score;
        private final List<String> targetConcerns;

        ScoredProduct(String productId, double score, List<String> targetConcerns) {
            this.productId      = productId;
            this.score          = score;
            this.targetConcerns = targetConcerns;
        }

        String getProductId()           { return productId; }
        double getScore()               { return score; }
        List<String> getTargetConcerns(){ return targetConcerns; }
    }
}
