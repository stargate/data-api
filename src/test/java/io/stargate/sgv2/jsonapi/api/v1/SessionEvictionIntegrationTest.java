package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import com.github.dockerjava.api.DockerClient;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = SessionEvictionIntegrationTest.SessionEvictionTestResource.class,
    restrictToAnnotatedClass = true)
public class SessionEvictionIntegrationTest extends AbstractCollectionIntegrationTestBase {

  /**
   * A specialized TestResource that spins up a new HCD/DSE container exclusively for this test
   * class.
   *
   * <p>Unlike the standard {@link DseTestResource} used by other tests, this resource ensures a
   * dedicated database instance. This isolation is crucial because this test involves destructive
   * operations that would negatively impact other tests sharing a common database.
   */
  public static class SessionEvictionTestResource extends DseTestResource {

    /**
     * Holds the reference to the container started by this resource.
     *
     * <p>This field is {@code static} to act as a bridge between the {@link QuarkusTestResource}
     * lifecycle (which manages the resource instance) and the test instance (where we need to
     * access the container to perform operations).
     */
    private static GenericContainer<?> sessionEvictionCassandraContainer;

    /**
     * Starts the container and captures the reference.
     *
     * <p>We override this method to capture the container instance created by the superclass into
     * our static {@link #sessionEvictionCassandraContainer} field, making it accessible to the test
     * methods.
     */
    @Override
    public Map<String, String> start() {
      Map<String, String> props = super.start();
      sessionEvictionCassandraContainer = super.getCassandraContainer();
      return props;
    }

    /**
     * Overridden to strictly prevent system property pollution.
     *
     * <p>The standard {@link DseTestResource} publishes connection details (like CQL port) to
     * global System Properties. Since this test runs in parallel with others, publishing our
     * isolated container's details would overwrite the shared container's configuration, causing
     * other tests to connect to this container (which we are about to kill), leading to random
     * failures in the test suite.
     */
    @Override
    protected void exposeSystemProperties(Map<String, String> props) {
      // No-op: Do not expose system properties to avoid interfering with other tests running in
      // parallel
    }

    public static GenericContainer<?> getSessionEvictionCassandraContainer() {
      return sessionEvictionCassandraContainer;
    }
  }

  /**
   * Overridden to ensure we connect to the isolated container created for this test.
   *
   * <p>The base class implementation relies on global system properties, which point to the shared
   * database container. To ensure this test correctly interacts with its dedicated container, we
   * must retrieve the port directly from the isolated container instance.
   */
  @Override
  protected int getCassandraCqlPort() {
    GenericContainer<?> container =
        SessionEvictionTestResource.getSessionEvictionCassandraContainer();
    if (container == null) {
      throw new IllegalStateException("Session eviction IT Cassandra container not started!");
    }
    return container.getMappedPort(9042);
  }

  @Test
  public void testSessionEvictionOnAllNodesFailed() throws Exception {

    // 1. Insert initial data to ensure the database is healthy before the test
    insertDoc(
        """
              {
                "insertOne": {
                  "document": {
                    "name": "before_crash"
                  }
                }
              }
              """);

    // 2. Stop the container to simulate DB failure
    // IMPORTANT: We use dockerClient.stopContainerCmd() instead of container.stop().
    // - container.stop() (Testcontainers) removes the container, so a restart would create a NEW
    //   container with a NEW random port.
    // - dockerClient.stopContainerCmd() (Docker API) simply halts the process but keeps the
    //   container (and its port mapping) intact.
    GenericContainer<?> dbContainer =
        SessionEvictionTestResource.getSessionEvictionCassandraContainer();
    DockerClient dockerClient = dbContainer.getDockerClient();
    String containerId = dbContainer.getContainerId();

    Log.info("Stopping Database Container to simulate failure...");
    dockerClient.stopContainerCmd(containerId).exec();

    try {
      // 3. Verify failure: The application should receive a 500 error/AllNodesFailedException
      givenHeadersPostJsonThen(
              """
                    {
                      "insertOne": {
                        "document": {
                          "name": "after_crash"
                        }
                      }
                    }
                    """)
          .statusCode(500)
          .body("errors[0].message", containsString("AllNodesFailedException"));

    } finally {
      // 4. Restart the container to simulate recovery
      Log.info("Restarting Database Container to simulate recovery...");
      dockerClient.startContainerCmd(containerId).exec();
    }

    // 5. Wait for the database to become responsive again
    waitForDbRecovery();

    // 6. Verify Session Recovery: The application should have evicted the bad session
    // and created a new one automatically.
    insertDoc(
        """
              {
                "insertOne": {
                  "document": {
                    "name": "after_recovery"
                  }
                }
              }
              """);

    Log.info("Test Passed: Session recovered after DB restart.");
  }

  /** Polls the database until it becomes responsive again. */
  private void waitForDbRecovery() {
    Log.info("Waiting for DB to recover...");
    long start = System.currentTimeMillis();
    long timeout = 60000; // 60 seconds timeout

    while (System.currentTimeMillis() - start < timeout) {
      try {
        // Perform a lightweight check (e.g., an empty find) to see if DB responds
        String json =
            """
              {
                "find": {
                  "filter": {}
                }
              }
              """;

        int statusCode =
            given()
                .port(getTestPort())
                .headers(getHeaders())
                .contentType(io.restassured.http.ContentType.JSON)
                .body(json)
                .when()
                .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
                .getStatusCode();

        // 200 OK means the DB handled the request (even if empty result)
        if (statusCode == 200) {
          Log.info("DB recovered!");
          return;
        }
      } catch (Exception e) {
        // Ignore connection errors and continue retrying
      }

      try {
        Thread.sleep(1000); // Poll every 1s
      } catch (InterruptedException ignored) {
      }
    }
    throw new RuntimeException("DB failed to recover within " + timeout + "ms");
  }
}
