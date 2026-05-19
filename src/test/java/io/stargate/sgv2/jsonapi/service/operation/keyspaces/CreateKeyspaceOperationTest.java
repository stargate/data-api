package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link CreateKeyspaceOperation} produces CQL via the driver's {@code SchemaBuilder}
 * rather than via {@code String.format} interpolation.
 *
 * <p>This removes the keyspace-name interpolation sink at the root: {@code CqlIdentifier} doubles
 * embedded double quotes, so a hostile keyspace name cannot break out of the identifier the way the
 * prior {@code "CREATE KEYSPACE \"%s\""} pattern allowed.
 *
 * <p><b>Note on datacenter names:</b> the driver's {@code
 * SchemaBuilder.withNetworkTopologyStrategy(Map)} does <em>not</em> escape map keys (see {@code
 * OptionsUtils.extractOptionValue} in {@code java-driver-query-builder}). The DC-name allowlist
 * applied by the resolver therefore remains the actual security control for the replication map;
 * the SchemaBuilder conversion only fixes the keyspace-identifier sink.
 */
class CreateKeyspaceOperationTest {

  @Nested
  class SimpleStrategy {

    @Test
    public void defaultsToReplicationFactorOne() {
      var op = new CreateKeyspaceOperation("red_star_belgrade", null, null);
      String cql = op.buildStatement().getQuery();
      assertThat(cql)
          .contains("CREATE KEYSPACE IF NOT EXISTS")
          .contains("red_star_belgrade")
          .contains("'class':'SimpleStrategy'")
          .contains("'replication_factor':1");
    }

    @Test
    public void honoursExplicitReplicationFactor() {
      var op =
          new CreateKeyspaceOperation(
              "red_star_belgrade", "SimpleStrategy", Map.of("replication_factor", 5));
      String cql = op.buildStatement().getQuery();
      assertThat(cql).contains("'class':'SimpleStrategy'").contains("'replication_factor':5");
    }

    @Test
    public void unknownStrategyFallsBackToSimple() {
      var op = new CreateKeyspaceOperation("k", "SomeOtherStrategy", null);
      String cql = op.buildStatement().getQuery();
      assertThat(cql).contains("'class':'SimpleStrategy'");
    }
  }

  @Nested
  class NetworkTopologyStrategy {

    @Test
    public void supportsRealisticCloudDataCenterNames() {
      var op = new CreateKeyspaceOperation("ks", "NetworkTopologyStrategy", Map.of("us-east-1", 3));
      String cql = op.buildStatement().getQuery();
      assertThat(cql).contains("'us-east-1':3");
    }

    @Test
    public void allowsEmptyDataCenterMap() {
      var op = new CreateKeyspaceOperation("ks", "NetworkTopologyStrategy", Map.of());
      String cql = op.buildStatement().getQuery();
      assertThat(cql).contains("'class':'NetworkTopologyStrategy'");
    }

    @Test
    public void driverDoesNotEscapeDataCenterMapKeys() {
      // Pinning behaviour: the driver's SchemaBuilder leaves map keys unescaped, so the
      // resolver-level DC-name allowlist is the actual defense against injection here. If this
      // test ever starts failing because the driver begins escaping, the allowlist becomes
      // defense-in-depth and the resolver wiring can be revisited.
      var op =
          new CreateKeyspaceOperation("ks", "NetworkTopologyStrategy", Map.of("dc'); EVIL --", 1));
      String cql = op.buildStatement().getQuery();
      assertThat(cql).contains("'dc'); EVIL --':1");
    }
  }

  @Nested
  class KeyspaceIdentifier {

    @Test
    public void unquotedForSimpleAsciiName() {
      var op = new CreateKeyspaceOperation("simple_name", null, null);
      String cql = op.buildStatement().getQuery();
      assertThat(cql).contains("CREATE KEYSPACE IF NOT EXISTS simple_name");
    }

    @Test
    public void escapesEmbeddedDoubleQuote() {
      // CqlIdentifier.fromInternal escapes embedded double quotes by doubling them, so a name
      // containing a quote cannot break out of the identifier the way the prior String.format
      // ("\"%s\"") sink allowed. This is the root-cause defense the previous fix was missing.
      var op = new CreateKeyspaceOperation("foo\"bar", null, null);
      String cql = op.buildStatement().getQuery();
      assertThat(cql).contains("\"foo\"\"bar\"");
    }
  }
}
