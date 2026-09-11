package io.stargate.sgv2.jsonapi.testresource;

import static io.stargate.sgv2.jsonapi.util.SmallRyeConfigTestUtil.addPropertyTo;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts an S3Mock container (https://github.com/adobe/S3Mock) and configures billing to send
 * evnets with short pauses.
 */
public class S3MockTestResource implements QuarkusTestResourceLifecycleManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3MockTestResource.class);

  /** Container tag; keep in sync with the {@code s3mock-testcontainers} version in pom.xml. */
  private static final String S3MOCK_VERSION = "5.1.0";

  public static final String BUCKET = "billing-events-it";
  public static final String REGION = "us-east-1";
  public static final String ACCESS_KEY = "s3mock-test";
  public static final String SECRET_KEY = "s3mock-test";

  private static volatile String httpEndpoint;

  private static volatile S3MockContainer container;

  /**
   * HTTP endpoint of the running S3Mock, for the test-side verification client.
   *
   * <p>...
   */
  public static String endpoint() {
    if (httpEndpoint == null) {
      throw new IllegalStateException("S3MockTestResource has not been started");
    }
    return httpEndpoint;
  }

  /**
   * Stops the S3Mock container, leaving nothing listening on the exported endpoint: every upload
   * from then on fails with connection-refused.
   */
  public static void stopContainer() {
    if (container == null) {
      throw new IllegalStateException("S3MockTestResource has not been started");
    }
    container.stop();
  }

  @Override
  public Map<String, String> start() {

    LOGGER.info(
        "start() - starting S3MockContainer. S3MOCK_VERSION:{}, BUCKET:{}", S3MOCK_VERSION, BUCKET);
    container = new S3MockContainer(S3MOCK_VERSION).withInitialBuckets(BUCKET);
    container.start();
    httpEndpoint = container.getHttpEndpoint();
    LOGGER.info(
        "start() - S3MockContainer started, httpEndpoint:{}, container:{}",
        httpEndpoint,
        container);

    // Building the config properties for the S3 billing component, these will
    // override whatever is it the test source tree, and enable S3 push with the
    // config we need for testing
    Map<String, String> props = new HashMap<>();

    // Enable Billing events at the feature flag level
    props.put("stargate.feature.flags.billing-events-logging", "true");

    addPropertyTo(props, BillingS3ExportConfig::enabled, "true");
    addPropertyTo(props, BillingS3ExportConfig::region, REGION);
    addPropertyTo(props, BillingS3ExportConfig::bucket, BUCKET);
    addPropertyTo(props, BillingS3ExportConfig::endpointOverride, httpEndpoint);

    // reducing the buffer so we get events sent more frequently
    addPropertyTo(props, BillingS3ExportConfig::bufferMaxBatchSize, 2);
    addPropertyTo(props, BillingS3ExportConfig::bufferMaxBatchBytes, 1024);
    addPropertyTo(props, BillingS3ExportConfig::bufferMaxBatchAge, "PT2S"); // 2 seconds

    // config the handler to wake up more often and be more aggressive in shutdown
    // wake up every 2 seconds to check the buffer
    addPropertyTo(props, BillingS3ExportConfig::handlerSleepDuration, "PT2S"); // 2 seconds

    // The uploader resolves credentials from the SDK default chain, whose first stop is the
    // system-property provider. S3Mock accepts any signed request.
    props.put("aws.accessKeyId", ACCESS_KEY);
    props.put("aws.secretAccessKey", SECRET_KEY);

    props.forEach(System::setProperty);
    LOGGER.info("start() - overridden properties for billing and S3. props:{}", props);
    return props;
  }

  @Override
  public void stop() {
    if (container != null) {
      LOGGER.info("stop() - stopping container. container:{}", container);
      container.stop();
      LOGGER.info("stop() - stopped container. container:{}", container);
    } else {
      LOGGER.info("stop() - container not running, nothing to do");
    }
  }
}
