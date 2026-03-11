package com.nuskin.prysm.flink.transform;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;

/**
 * Encrypts sensitive (PII) fields in-place within the Avro
 * {@link GenericRecord} before the event is written to Iceberg.
 *
 * <h3>Encryption scheme: Envelope encryption with AWS KMS</h3>
 * <ol>
 *   <li>On startup, a Data Encryption Key (DEK) is generated via
 *       {@code kms:GenerateDataKey}. The plaintext DEK is kept in
 *       memory; the encrypted DEK is stored alongside the ciphertext.</li>
 *   <li>Each sensitive field value is encrypted with AES-256-GCM
 *       (authenticated encryption).</li>
 *   <li>The stored value format is Base64({encryptedDek}||{iv}||{ciphertext}),
 *       all the information needed for decryption is self-contained.</li>
 * </ol>
 *
 * <p>DEKs are rotated per job restart. For field-level key isolation
 * (different key per customer), extend this class to use per-customer DEKs
 * cached by customerId.
 *
 * <h3>Pseudonymisation</h3>
 * <p>The {@code customerId} field in analytics-facing tables is replaced with
 * an HMAC-SHA256 pseudonym derived from the raw ID + a secret salt from
 * Secrets Manager. The mapping is maintained separately in the raw table
 * which has stricter IAM access.
 */
public class PIIEncryptionTransform extends RichMapFunction<RawStreamEvent, RawStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PIIEncryptionTransform.class);
    private static final int GCM_IV_LENGTH   = 12;
    private static final int GCM_TAG_LENGTH  = 128;

    private final AppConfig appConfig;
    private transient KmsClient kmsClient;
    private transient SecretKeySpec aesKey;
    private transient byte[] encryptedDek;
    private transient SecureRandom secureRandom;

    public PIIEncryptionTransform(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (!appConfig.isPiiEncryptionEnabled()) {
            LOG.warn("PII encryption is DISABLED — not suitable for production");
            return;
        }
        kmsClient = KmsClient.builder()
                .region(Region.of(appConfig.getRegion()))
                .build();

        // Generate a data key from KMS
        GenerateDataKeyResponse dkResponse = kmsClient.generateDataKey(
                GenerateDataKeyRequest.builder()
                        .keyId(appConfig.getPiiKmsKeyArn())
                        .keySpec(DataKeySpec.AES_256)
                        .build());

        byte[] plaintextDek = dkResponse.plaintext().asByteArray();
        encryptedDek = dkResponse.ciphertextBlob().asByteArray();
        aesKey = new SecretKeySpec(plaintextDek, "AES");
        secureRandom = new SecureRandom();

        LOG.info("PII encryption initialised — DEK generated via KMS key: {}",
                 appConfig.getPiiKmsKeyArn());
    }

    @Override
    public RawStreamEvent map(RawStreamEvent event) throws Exception {
        List<String> sensitiveFields = event.getStreamDefinition().getSensitiveFields();
        if (sensitiveFields.isEmpty() || !appConfig.isPiiEncryptionEnabled()) {
            return event;
        }

        GenericRecord original = event.getAvroRecord();
        // Create a mutable copy so we don't mutate the source record
        GenericRecord encrypted = deepCopy(original);

        for (String fieldName : sensitiveFields) {
            Schema.Field field = encrypted.getSchema().getField(fieldName);
            if (field == null) continue; // Field may not exist in this schema version

            Object value = encrypted.get(fieldName);
            if (value == null) continue;

            String plaintext = value.toString();
            String ciphertext = encryptField(plaintext);
            encrypted.put(fieldName, ciphertext);
        }

        return new RawStreamEvent(
                event.getStreamKey(),
                event.getStreamDefinition(),
                encrypted);
    }

    @Override
    public void close() {
        if (kmsClient != null) {
            kmsClient.close();
        }
    }

    // ── Encryption helpers ───────────────────────────────────────────────────

    /**
     * Encrypts a plaintext string using AES-256-GCM.
     * Output format: Base64(encryptedDekLength[4 bytes] || encryptedDek || iv[12] || ciphertext)
     */
    private String encryptField(String plaintext) throws Exception {
        byte[] iv = new byte[GCM_IV_LENGTH];
        secureRandom.nextBytes(iv);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
        byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

        // Layout: [4-byte DEK length][encrypted DEK][12-byte IV][ciphertext+tag]
        ByteBuffer buf = ByteBuffer.allocate(4 + encryptedDek.length + iv.length + ciphertext.length);
        buf.putInt(encryptedDek.length);
        buf.put(encryptedDek);
        buf.put(iv);
        buf.put(ciphertext);

        return Base64.getEncoder().encodeToString(buf.array());
    }

    /**
     * Decrypts a field value encrypted by {@link #encryptField}.
     * Used by downstream consumers with KMS decrypt permissions.
     */
    public String decryptField(String encodedCiphertext) throws Exception {
        byte[] raw = Base64.getDecoder().decode(encodedCiphertext);
        ByteBuffer buf = ByteBuffer.wrap(raw);

        int dekLength = buf.getInt();
        byte[] encDek = new byte[dekLength];
        buf.get(encDek);

        // Decrypt DEK with KMS
        byte[] plaintextDek = kmsClient.decrypt(DecryptRequest.builder()
                .ciphertextBlob(SdkBytes.fromByteArray(encDek))
                .keyId(appConfig.getPiiKmsKeyArn())
                .build())
                .plaintext().asByteArray();

        byte[] iv = new byte[GCM_IV_LENGTH];
        buf.get(iv);

        byte[] ciphertext = new byte[buf.remaining()];
        buf.get(ciphertext);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(plaintextDek, "AES"),
                    new GCMParameterSpec(GCM_TAG_LENGTH, iv));
        return new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8);
    }

    private GenericRecord deepCopy(GenericRecord original) {
        return GenericData.get().deepCopy(original.getSchema(), original);
    }
}
