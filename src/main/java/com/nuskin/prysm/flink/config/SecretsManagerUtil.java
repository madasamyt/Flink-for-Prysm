package com.nuskin.prysm.flink.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import java.util.HashMap;
import java.util.Map;

/**
 * Wraps AWS Secrets Manager for retrieving runtime credentials and secret keys.
 *
 * <h3>What lives in Secrets Manager vs SSM Parameter Store</h3>
 * <ul>
 *   <li><b>Secrets Manager:</b> HMAC salts, API keys, anything that is rotated
 *       or must never appear in logs under any circumstance.</li>
 *   <li><b>SSM Parameter Store (SecureString):</b> KMS key ARNs, bucket names,
 *       stream names — infrastructure config that changes with environment but
 *       is not itself a credential.</li>
 * </ul>
 *
 * <p>Secrets are cached within a single JVM invocation. Jobs requiring
 * rotation support should be restarted; alternatively, implement a
 * periodic cache-invalidation mechanism with a TTL.
 */
public class SecretsManagerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SecretsManagerUtil.class);

    private final SecretsManagerClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** Cache — avoids multiple API calls for the same secret in one JVM lifecycle */
    private final Map<String, String> cache = new HashMap<>();

    public SecretsManagerUtil(String region) {
        this.client = SecretsManagerClient.builder()
                .region(Region.of(region))
                .build();
    }

    /**
     * Returns the raw secret string for the given secret name.
     * Secrets are cached after the first call.
     *
     * @param secretName full ARN or friendly name, e.g.
     *                   {@code /prysm/prod/secrets/pii-hmac-secret}
     */
    public String getSecret(String secretName) {
        if (cache.containsKey(secretName)) {
            return cache.get(secretName);
        }
        try {
            LOG.info("Fetching secret: {}", secretName);
            GetSecretValueResponse response = client.getSecretValue(
                    GetSecretValueRequest.builder().secretId(secretName).build());

            String value = response.secretString() != null
                    ? response.secretString()
                    : new String(response.secretBinary().asByteArray());
            cache.put(secretName, value);
            return value;
        } catch (ResourceNotFoundException e) {
            throw new RuntimeException("Secret not found in Secrets Manager: " + secretName, e);
        }
    }

    /**
     * Retrieves a JSON secret and returns the value of the specified key.
     * Many Secrets Manager secrets are stored as JSON objects with multiple keys.
     *
     * @param secretName secret name / ARN
     * @param jsonKey    key within the JSON object
     */
    public String getSecretField(String secretName, String jsonKey) {
        String raw = getSecret(secretName);
        try {
            JsonNode node = objectMapper.readTree(raw);
            JsonNode field = node.get(jsonKey);
            if (field == null) {
                throw new RuntimeException(
                        "Key '" + jsonKey + "' not found in secret '" + secretName + "'");
            }
            return field.asText();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse JSON secret '" + secretName + "'", e);
        }
    }

    /**
     * Retrieves a JSON secret and returns all key-value pairs.
     * Convenient for secrets that bundle multiple related values.
     */
    public Map<String, String> getSecretAsMap(String secretName) {
        String raw = getSecret(secretName);
        try {
            JsonNode node = objectMapper.readTree(raw);
            Map<String, String> result = new HashMap<>();
            node.fields().forEachRemaining(e -> result.put(e.getKey(), e.getValue().asText()));
            return result;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse JSON secret '" + secretName + "' as map", e);
        }
    }

    /** Invalidates the local cache, forcing the next call to re-fetch from AWS */
    public void invalidateCache() {
        LOG.info("Invalidating Secrets Manager cache ({} entries)", cache.size());
        cache.clear();
    }

    public void close() {
        client.close();
    }
}
