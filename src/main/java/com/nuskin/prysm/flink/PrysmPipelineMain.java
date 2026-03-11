package com.nuskin.prysm.flink;

import com.nuskin.prysm.flink.job.IngestionJob;
import com.nuskin.prysm.flink.job.RecommendationJob;
import com.nuskin.prysm.flink.job.ScanAnalyticsJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the fat JAR.
 *
 * <p>Select which job to run via the {@code --job} argument:
 * <pre>
 *   flink run -c com.nuskin.prysm.flink.PrysmPipelineMain \
 *       target/flink-prysm-pipeline-1.0.0-fat.jar \
 *       --job ingestion        # reads all 19 streams → Iceberg
 *       --job analytics        # scan metrics aggregation + windowed baselines
 *       --job recommendation   # scan snapshots + product catalogue → recommendations
 * </pre>
 *
 * In production each job typically runs as a separate Flink application
 * (separate JARs or separate job clusters) for independent scaling and
 * fault isolation. This single entry-point is convenient for local development.
 */
public class PrysmPipelineMain {

    private static final Logger LOG = LoggerFactory.getLogger(PrysmPipelineMain.class);

    public static void main(String[] args) throws Exception {
        String jobType = parseJobArg(args);
        LOG.info("Starting Prysm pipeline job: {}", jobType);

        switch (jobType) {
            case "ingestion":
                new IngestionJob().run();
                break;
            case "analytics":
                new ScanAnalyticsJob().run();
                break;
            case "recommendation":
                new RecommendationJob().run();
                break;
            default:
                System.err.println("Usage: --job <ingestion|analytics|recommendation>");
                System.exit(1);
        }
    }

    private static String parseJobArg(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--job".equals(args[i])) {
                return args[i + 1].toLowerCase();
            }
        }
        // Default to ingestion if no --job arg supplied
        LOG.warn("No --job argument supplied, defaulting to ingestion");
        return "ingestion";
    }
}
