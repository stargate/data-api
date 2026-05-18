package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs billing events as structured JSON via a dedicated {@code billing.events} logger.
 *
 * <p>Only emits events when the tenant is Astra. Downstream billing pipelines parse these log lines
 * for invoicing.
 */
public final class BillingEventLogger {

  private static final Logger BILLING_LOGGER = LoggerFactory.getLogger("billing.events");
  private static final Logger LOGGER = LoggerFactory.getLogger(BillingEventLogger.class);
  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";

  private BillingEventLogger() {}

  /**
   * Logs a billing event for the given aggregated model usage. No-ops if the tenant is not Astra.
   */
  public static void logBillingEvent(ModelUsage modelUsage) {
    if (modelUsage.tenant().databaseType() != DatabaseType.ASTRA) {
      return;
    }
    try {
      BillingEvent event = BillingEvent.from(modelUsage, PRODUCT, RESOURCE_TYPE);
      BILLING_LOGGER.info(OBJECT_WRITER.writeValueAsString(event));
    } catch (JacksonException e) {
      LOGGER.error("Failed to serialize billing event", e);
    }
  }
}
