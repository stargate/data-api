package io.stargate.sgv2.jsonapi.testresource;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts an S3Mock container and enables the billing S3 export against it, with small batch
 * thresholds so tests see objects quickly. Used by {@code BillingS3ExportIntegrationTest} alongside
 * {@link DseTestResource}.
 *
 * <p>The returned properties reach the application under test (a separate process for
 * {@code @QuarkusIntegrationTest}); mirroring them as system properties follows the {@link
 * StargateTestResource} pattern so the whole test environment sees the same values.
 */
public class S3MockTestResource implements QuarkusTestResourceLifecycleManager {

  private static final Logger LOG = LoggerFactory.getLogger(S3MockTestResource.class);

  /** Container tag; keep in sync with the {@code s3mock-testcontainers} version in pom.xml. */
  private static final String S3MOCK_VERSION = "5.1.0";

  public static final String BUCKET = "billing-events-it";
  public static final String BUCKET_REGION = "us-east-1";
  public static final String ACCESS_KEY = "s3mock-test";
  public static final String SECRET_KEY = "s3mock-test";

  private static volatile String httpEndpoint;

  private static volatile S3MockContainer container;

  /** HTTP endpoint of the running S3Mock, for the test-side verification client. */
  public static String endpoint() {
    if (httpEndpoint == null) {
      throw new IllegalStateException("S3MockTestResource has not been started");
    }
    return httpEndpoint;
  }

  /**
   * Stops the S3Mock container, leaving nothing listening on the exported endpoint: every upload
   * from then on fails with connection-refused, like an S3 outage. One-way for the whole test class
   * (a restart would map a new port, unreachable through the app's fixed endpoint-override), so
   * only the last test may call this.
   */
  public static void stopContainer() {
    if (container == null) {
      throw new IllegalStateException("S3MockTestResource has not been started");
    }
    container.stop();
  }

  @Override
  public Map<String, String> start() {
    container = new S3MockContainer(S3MOCK_VERSION).withInitialBuckets(BUCKET);
    container.start();
    httpEndpoint = container.getHttpEndpoint();

    Map<String, String> props = new HashMap<>();
    props.put("stargate.jsonapi.billing.s3.enabled", "true");
    props.put("stargate.jsonapi.billing.s3.bucket", BUCKET);
    props.put("stargate.jsonapi.billing.s3.bucket-region", BUCKET_REGION);
    props.put("stargate.jsonapi.billing.s3.endpoint-override", httpEndpoint);
    // Small thresholds so the export flushes promptly: count seal at 5, age sweep every 2s.
    props.put("stargate.jsonapi.billing.s3.max-events", "5");
    props.put("stargate.jsonapi.billing.s3.max-age", "PT2S");
    props.put("stargate.jsonapi.billing.s3.shutdown-timeout", "PT5S");
    // The producer side (DefaultBilling) is feature-flagged off by default.
    props.put("stargate.feature.flags.billing-events-logging", "true");
    // The uploader resolves credentials from the SDK default chain, whose first stop is the
    // system-property provider. S3Mock accepts any signed request.
    props.put("aws.accessKeyId", ACCESS_KEY);
    props.put("aws.secretAccessKey", SECRET_KEY);

    props.forEach(System::setProperty);
    LOG.info("S3Mock started for billing export IT: endpoint={}, bucket={}", httpEndpoint, BUCKET);
    return props;
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }
}
