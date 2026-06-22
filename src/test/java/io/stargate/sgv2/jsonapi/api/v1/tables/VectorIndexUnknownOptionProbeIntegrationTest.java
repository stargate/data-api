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
 * Probe (issue #2487): does the backing DB (DSE 6.9 / HCD) accept an unknown option key in a vector
 * SAI index's {@code CREATE CUSTOM INDEX ... WITH OPTIONS}?
 *
 * <p>Issues raw CQL via the admin {@link CqlSession} from {@link
 * AbstractKeyspaceIntegrationTestBase}, bypassing the {@code ApiVectorIndex.applyIndexingOptions}
 * allow-list and the data-api HTTP command layer.
 *
 * <p>Hypothesis: an unknown key (here {@code profile}) is rejected by SAI option validation
 * regardless of {@code SAI_HNSW_ALLOW_CUSTOM_PARAMETERS}, which only gates known HNSW tuning params
 * like {@code maximum_node_connections}. The control index uses only {@code
 * similarity_function:cosine} to confirm the table/column/CQL is otherwise valid.
 *
 * <p>Always passes, recording observed behavior to stdout. Flip {@code EXPECT_UNKNOWN_KEY_REJECTED}
 * to make it a hard assertion once the answer is known.
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

  /** Reflective accessor to the base class's private CqlSession, for direct error inspection. */
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
    // Control: only a known-good SAI option. Must succeed, so an unknown-key failure in the test
    // case is attributable to the unknown key, not the table/column/CQL path.
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
    // Adds an unknown key ('profile'); similarity_function is kept so the only difference vs the
    // control is that key.
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
