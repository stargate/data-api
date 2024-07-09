package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.metrics.DefaultMetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricIdGenerator;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * Customized Tagging Metric ID Generator for Driver Metric
 *
 * <p>Session metric identifiers contain a name starting with "session." and ending with the metric
 * path, a tag with the key "session" and the value of the current session name, a tag with the key
 * 'tenant' and the value of current tenantId.
 *
 * <p>Node metric identifiers contain a name starting with "nodes." and ending with the metric path,
 * and 3 tags: a tag with the key "session" and the value of the current session name, a tag with
 * the key "node" and the value of the current node endpoint, a tag with the key 'tenant' and the
 * value of current tenantId.
 */
public class CustomTaggingMetricIdGenerator implements MetricIdGenerator {

  private final String sessionName;
  private final String sessionPrefix;
  private final String nodePrefix;
  private final String tenantId;

  @SuppressWarnings("unused")
  public CustomTaggingMetricIdGenerator(DriverContext context) {
    sessionName = context.getSessionName();
    String prefix =
        Objects.requireNonNull(
            context
                .getConfig()
                .getDefaultProfile()
                .getString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, ""));
    sessionPrefix = prefix.isEmpty() ? "session." : prefix + ".session.";
    nodePrefix = prefix.isEmpty() ? "nodes." : prefix + ".nodes.";
    tenantId =
        ((TenantAwareCqlSessionBuilder.TenantAwareDriverContext) context)
            .getStartupOptions()
            .get(TenantAwareCqlSessionBuilder.TENANT_ID_PROPERTY_KEY);
  }

  @NonNull
  @Override
  public MetricId sessionMetricId(@NonNull SessionMetric metric) {
    return new DefaultMetricId(
        sessionPrefix + metric.getPath(), ImmutableMap.of("tenant", tenantId));
  }

  @NonNull
  @Override
  public MetricId nodeMetricId(@NonNull Node node, @NonNull NodeMetric metric) {
    return new DefaultMetricId(
        nodePrefix + metric.getPath(),
        ImmutableMap.of("node", node.getEndPoint().toString(), "tenant", tenantId));
  }
}
