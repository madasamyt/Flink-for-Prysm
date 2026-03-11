package com.nuskin.prysm.flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersByPathRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersByPathResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;
import software.amazon.awssdk.services.ssm.model.ParameterNotFoundException;

import java.util.HashMap;
import java.util.Map;

/**
 * Thin wrapper around AWS SSM Parameter Store.
 *
 * <p>All non-sensitive infrastructure configuration (stream names, bucket names,
 * KMS key ARNs) is stored in SSM under the path prefix
 * {@code /prysm/{env}/config/}. This keeps configuration out of the container
 * image and away from source control while still allowing IAM-scoped access.
 *
 * <p><b>Security note:</b> SecureString parameters (encrypted by KMS) are used
 * for any values that, while not themselves credentials, are sensitive enough
 * to warrant encryption at rest in SSM (e.g. KMS key ARNs, account IDs).
 */
public class ParameterStoreUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ParameterStoreUtil.class);

    private final SsmClient ssmClient;
    private final String env;
    /** Local cache to avoid repeated SSM calls within a single JVM */
    private final Map<String, String> cache = new HashMap<>();

    public ParameterStoreUtil(String region, String env) {
        this.env = env;
        this.ssmClient = SsmClient.builder()
                .region(Region.of(region))
                .build();
    }

    /**
     * Fetches a single SSM parameter.
     *
     * @param path Full SSM path (e.g. {@code /prysm/prod/config/data-lake-bucket})
     * @param withDecryption {@code true} for SecureString parameters
     * @return parameter value
     * @throws RuntimeException if the parameter does not exist
     */
    public String getParameter(String path, boolean withDecryption) {
        String resolved = resolvePath(path);
        if (cache.containsKey(resolved)) {
            return cache.get(resolved);
        }
        try {
            LOG.info("Fetching SSM parameter: {}", resolved);
            String value = ssmClient.getParameter(GetParameterRequest.builder()
                            .name(resolved)
                            .withDecryption(withDecryption)
                            .build())
                    .parameter()
                    .value();
            cache.put(resolved, value);
            return value;
        } catch (ParameterNotFoundException e) {
            throw new RuntimeException("Required SSM parameter not found: " + resolved, e);
        }
    }

    /** Shorthand for plaintext String parameters */
    public String getParameter(String path) {
        return getParameter(path, false);
    }

    /** Shorthand for SecureString parameters */
    public String getSecureParameter(String path) {
        return getParameter(path, true);
    }

    /**
     * Fetches all parameters under a given path prefix recursively.
     * Useful for bulk-loading stream names or feature flags.
     *
     * @param pathPrefix e.g. {@code /prysm/dev/config/}
     * @return map of parameterName → value
     */
    public Map<String, String> getParametersByPath(String pathPrefix) {
        String resolved = resolvePath(pathPrefix);
        Map<String, String> result = new HashMap<>();
        String nextToken = null;

        do {
            GetParametersByPathRequest.Builder req = GetParametersByPathRequest.builder()
                    .path(resolved)
                    .recursive(true)
                    .withDecryption(true);
            if (nextToken != null) {
                req.nextToken(nextToken);
            }
            GetParametersByPathResponse response = ssmClient.getParametersByPath(req.build());
            for (Parameter param : response.parameters()) {
                result.put(param.name(), param.value());
                cache.put(param.name(), param.value());
            }
            nextToken = response.nextToken();
        } while (nextToken != null);

        LOG.info("Loaded {} parameters from SSM path: {}", result.size(), resolved);
        return result;
    }

    /**
     * Resolves the {@code {env}} placeholder in SSM path templates defined in
     * {@code streams-topology.yml}.
     */
    public String resolvePath(String pathTemplate) {
        return pathTemplate.replace("{env}", env);
    }

    public void close() {
        ssmClient.close();
    }
}
