package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import java.lang.reflect.Method;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * EMPIRICAL PROBE (issue #2487): Does the backing DB (DSE 6.9 / HCD) accept an UNKNOWN option KEY
 * in a vector SAI index's {@code CREATE CUSTOM INDEX ... WITH OPTIONS}?
 *
 * <p>This BYPASSES data-api's own {@code ApiVectorIndex.applyIndexingOptions} allow-list by issuing
 * RAW CQL directly against the running test container via the admin {@link CqlSession} provided by
 * {@link AbstractKeyspaceIntegrationTestBase} (driver session, {@code cassandra/cassandra}). It
 * does NOT go through the data-api HTTP command layer.
 *
 * <p>Hypothesis: a key SAI has never heard of (here {@code profile}) should be rejected by SAI's
 * option validation regardless of {@code SAI_HNSW_ALLOW_CUSTOM_PARAMETERS} (that flag only gates
 * the KNOWN custom HNSW tuning params like {@code maximum_node_connections}). The control index,
 * using only {@code similarity_function:cosine}, must succeed to prove the table/column/CQL is
 * otherwise valid.
 *
 * <p>The test is written to ALWAYS PASS while RECORDING the observed behavior to stdout, so the
 * probe never fails CI ambiguously; flip {@code EXPECT_UNKNOWN_KEY_REJECTED} to turn it into a hard
 * assertion once the empirical answer is known.
 */
@QuarkusIntegrationTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class VectorIndexUnknownOptionProbeIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  /** Flip to true to turn the probe into a hard assertion that the unknown key is rejected. */
  private static final boolean EXPECT_UNKNOWN_KEY_REJECTED = false;

  private static final String TABLE =
      "probe_" + RandomStringUtils.insecure().nextAlphanumeric(12).toLowerCase();
  private static final String VECTOR_COL = "embedding";
  private static final int DIMENSION = 4;

  /**
   * Reflective accessor to the private CqlSession in the base class, for direct error inspection.
   */
  private CqlSession session() {
    try {
      Method m = AbstractKeyspaceIntegrationTestBase.class.getDeclaredMethod("createDriverSession");
      m.setAccessible(true);
      return (CqlSession) m.invoke(this);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not obtain CqlSession from base class", e);
    }
  }

  @Test
  @Order(1)
  void createVectorTable() {
    // Raw CQL: keyspace already created by AbstractKeyspaceIntegrationTestBase#createKeyspace.
    boolean applied =
        executeCqlStatement(
            String.format(
                "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" "
                    + "(id text PRIMARY KEY, %s vector<float, %d>)",
                keyspaceName, TABLE, VECTOR_COL, DIMENSION));
    assertThat(applied).as("vector table create applied").isTrue();
  }

  @Test
  @Order(2)
  void controlIndex_knownGoodOptionsOnly_mustSucceed() {
    // CONTROL: only a known-good SAI option. Must succeed -> proves table/column/CQL path is valid
    // and that an unknown-key failure in the TEST case is specifically about the unknown key.
    String cql =
        String.format(
            "CREATE CUSTOM INDEX \"idx_control_%s\" ON \"%s\".\"%s\" (%s) "
                + "USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function':'cosine'}",
            TABLE, keyspaceName, TABLE, VECTOR_COL);

    assertThatCode(() -> session().execute(SimpleStatement.newInstance(cql)))
        .as("CONTROL index with only {'similarity_function':'cosine'} must be accepted")
        .doesNotThrowAnyException();
  }

  @Test
  @Order(3)
  void testIndex_unknownOptionKey_recordAcceptOrReject() {
    // TEST: add an option key SAI does not know about ('profile'). similarity_function is kept so
    // the ONLY difference vs the control is the unknown key.
    String cql =
        String.format(
            "CREATE CUSTOM INDEX \"idx_test_%s\" ON \"%s\".\"%s\" (%s) "
                + "USING 'StorageAttachedIndex' "
                + "WITH OPTIONS = {'similarity_function':'cosine','profile':'small-high-recall'}",
            TABLE, keyspaceName, TABLE, VECTOR_COL);

    Throwable thrown =
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(
            () -> {
              try {
                session().execute(SimpleStatement.newInstance(cql));
                return (Throwable) null;
              } catch (Throwable t) {
                return t;
              }
            });

    boolean rejected = thrown != null;
    System.out.println("=================================================================");
    System.out.println(
        "[VECTOR-INDEX-UNKNOWN-OPTION PROBE] unknown key 'profile' rejected=" + rejected);
    if (rejected) {
      System.out.println("[PROBE] rejection class : " + thrown.getClass().getName());
      System.out.println("[PROBE] rejection message: " + thrown.getMessage());
    } else {
      System.out.println("[PROBE] DB SILENTLY ACCEPTED the unknown 'profile' key.");
    }
    System.out.println("=================================================================");

    if (EXPECT_UNKNOWN_KEY_REJECTED) {
      assertThat(rejected).as("DB should reject unknown SAI option key 'profile'").isTrue();
    }
  }
}
