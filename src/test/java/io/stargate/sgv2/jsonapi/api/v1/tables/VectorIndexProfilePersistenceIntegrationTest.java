package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorIndexProfileDefinition;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Happy-path IT for {@code vectorIndexing} profiles (#2487): creating a vector index with a named
 * profile records the profile in the table extensions ({@link
 * SchemaConstants.MetadataFieldsNames#VECTOR_INDEX_PROFILES}), and dropping the index removes it.
 *
 * <p>Creating the index emits custom SAI HNSW params (the profile's expanded options, e.g. {@code
 * maximum_node_connections}), which a cluster only accepts with {@code
 * SAI_HNSW_ALLOW_CUSTOM_PARAMETERS} enabled. The default {@code dse-server:6.9.21} lane rejects
 * them, so a capability probe runs in {@link #setup()} and the test is skipped where unsupported.
 *
 * <p>This complements the API-validation cases in {@link CreateTableIndexIntegrationTest}, which
 * are backend-agnostic; the create/persist and drop/cleanup DB paths only run where the cluster
 * allows custom params.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class VectorIndexProfilePersistenceIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String PROFILE = "small-high-recall";
  // What "small-high-recall" expands to (VectorIndexProfiles registry), stored as the snapshot.
  private static final Map<String, String> EXPECTED_OPTIONS =
      Map.of("maximum_node_connections", "32", "construction_beam_width", "200");

  private final String tableName =
      "vix_profile_" + RandomStringUtils.insecure().nextAlphanumeric(8).toLowerCase();
  private final String vectorColumn = "embedding";

  /** Whether the backing cluster accepts custom SAI HNSW params (probed in {@link #setup()}). */
  private boolean customParamsSupported;

  /** Reflective accessor to the base class's private admin CqlSession, to read schema metadata. */
  private CqlSession session() {
    try {
      Method m = AbstractKeyspaceIntegrationTestBase.class.getDeclaredMethod("createDriverSession");
      m.setAccessible(true);
      return (CqlSession) m.invoke(this);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not obtain CqlSession from base class", e);
    }
  }

  @BeforeAll
  void setup() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            tableName,
            Map.ofEntries(
                Map.entry("id", Map.of("type", "text")),
                Map.entry(vectorColumn, Map.of("type", "vector", "dimension", 4))),
            "id")
        .wasSuccessful();

    customParamsSupported = probeCustomSaiParamsSupported();
  }

  @Test
  void profilePersistedOnCreateAndRemovedOnDrop() {
    assumeTrue(
        customParamsSupported,
        "cluster does not allow custom SAI HNSW params (SAI_HNSW_ALLOW_CUSTOM_PARAMETERS)");

    String indexName = tableName + "_idx";

    assertTableCommand(keyspaceName, tableName)
        .postCreateVectorIndex(
                """
            {
              "name": "%s",
              "definition": {
                "column": "%s",
                "options": { "vectorIndexing": "%s" }
              }
            }
            """
                .formatted(indexName, vectorColumn, PROFILE))
        .wasSuccessful();

    var afterCreate = readProfiles();
    assertThat(afterCreate).as("profile recorded after create").containsKey(indexName);
    assertThat(afterCreate.get(indexName).profile()).isEqualTo(PROFILE);
    assertThat(afterCreate.get(indexName).options())
        .as("stored snapshot is the options the profile expanded to")
        .isEqualTo(EXPECTED_OPTIONS);

    assertNamespaceCommand(keyspaceName).templated().dropIndex(indexName, false).wasSuccessful();

    assertThat(readProfiles()).as("profile removed after drop").doesNotContainKey(indexName);
  }

  /**
   * Probes whether the cluster accepts custom SAI HNSW params by issuing raw CQL on a throwaway
   * table: a CREATE CUSTOM INDEX with a known tuning option. Returns false when the cluster rejects
   * it ({@link QueryValidationException}); the throwaway table is always dropped. Connection or
   * other errors propagate so a broken environment fails loudly rather than silently skipping.
   */
  private boolean probeCustomSaiParamsSupported() {
    String probeTable =
        "vix_probe_" + RandomStringUtils.insecure().nextAlphanumeric(8).toLowerCase();
    session()
        .execute(
            SimpleStatement.newInstance(
                String.format(
                    "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" "
                        + "(id text PRIMARY KEY, %s vector<float, 4>)",
                    keyspaceName, probeTable, vectorColumn)));
    try {
      session()
          .execute(
              SimpleStatement.newInstance(
                  String.format(
                      "CREATE CUSTOM INDEX \"%s_idx\" ON \"%s\".\"%s\" (%s) "
                          + "USING 'StorageAttachedIndex' "
                          + "WITH OPTIONS = {'similarity_function':'cosine',"
                          + "'maximum_node_connections':'16'}",
                      probeTable, keyspaceName, probeTable, vectorColumn)));
      return true;
    } catch (QueryValidationException e) {
      return false;
    } finally {
      session()
          .execute(
              SimpleStatement.newInstance(
                  String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", keyspaceName, probeTable)));
    }
  }

  /** Reads the VECTOR_INDEX_PROFILES extension off the table after refreshing schema metadata. */
  private Map<String, VectorIndexProfileDefinition> readProfiles() {
    try {
      session().refreshSchemaAsync().toCompletableFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("schema refresh interrupted", e);
    } catch (Exception e) {
      throw new RuntimeException("schema refresh failed", e);
    }
    KeyspaceMetadata keyspace =
        session()
            .getMetadata()
            .getKeyspace(CqlIdentifier.fromInternal(keyspaceName))
            .orElseThrow(() -> new RuntimeException("keyspace not found: " + keyspaceName));
    TableMetadata table =
        keyspace
            .getTable(CqlIdentifier.fromInternal(tableName))
            .orElseThrow(() -> new RuntimeException("table not found: " + tableName));
    return VectorIndexProfileDefinition.from(table, MAPPER);
  }
}
