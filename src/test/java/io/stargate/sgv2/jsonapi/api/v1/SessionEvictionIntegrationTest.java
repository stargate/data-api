package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static org.hamcrest.Matchers.*;

import com.github.dockerjava.api.DockerClient;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
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
  public void testSessionEvictionOnAllNodesFailed() {

    // 1. Insert and find initial data to ensure the database is healthy before the test
    insertDoc(
        """
              {
                "_id": "before_crash"
              }
              """);

    givenHeadersPostJsonThenOkNoErrors(
            """
              {
                "findOne": {
                  "filter" : {"_id" : "before_crash"}
                }
              }
              """)
        .body("$", responseIsFindSuccess())
        .body("data.document._id", is("before_crash"));

    // 2. Stop the container to simulate DB failure
    GenericContainer<?> dbContainer =
        SessionEvictionTestResource.getSessionEvictionCassandraContainer();
    DockerClient dockerClient = dbContainer.getDockerClient();
    String containerId = dbContainer.getContainerId();

    Log.error("Stopping Database Container to simulate failure...");
    Log.error(
        "Container ID: " + containerId + ", Port Before Stop: " + dbContainer.getMappedPort(9042));
    dockerClient.stopContainerCmd(containerId).exec();

    try {
      // 3. Verify failure: The application should receive a 500 error/AllNodesFailedException
      givenHeadersPostJsonThen(
              """
              {
                "findOne": {}
              }
              """)
          .statusCode(500)
          .body("errors[0].message", containsString("No node was available"));

    } finally {
      // 4. Restart the container to simulate recovery
      Log.error("Restarting Database Container to simulate recovery...");
      dockerClient.startContainerCmd(containerId).exec();
      Log.error(
          "Container ID: "
              + containerId
              + ", Port After Start: "
              + dbContainer.getMappedPort(9042));

      // check status
      var inspectAfter = dockerClient.inspectContainerCmd(containerId).exec();
      var state = inspectAfter.getState();
      Log.error(
          "Container Status After Start: "
              + dockerClient
                  .inspectContainerCmd(containerId)
                  .exec()
                  .getState()
                  .getStatus()); // 应该是 "running"
      Log.error("Container Running: " + state.getRunning());
    }

    // 5. Wait for the database to become responsive again
    waitForDbRecovery();

    // 6. Verify Session Recovery: The application should have evicted the bad session
    // and created a new one automatically.
    insertDoc(
        """
                {
                  "_id": "after_crash"
                }
                """);

    givenHeadersPostJsonThenOkNoErrors(
            """
            {
              "findOne": {
                "filter" : {"_id" : "after_crash"}
              }
            }
            """)
        .body("$", responseIsFindSuccess())
        .body("data.document._id", is("after_crash"));

    Log.error("Test Passed: Session recovered after DB restart.");
  }

  /** Polls the database until it becomes responsive again. */
  private void waitForDbRecovery() {
    Log.error("Waiting for DB to recover...");
    GenericContainer<?> dbContainer =
        SessionEvictionTestResource.getSessionEvictionCassandraContainer();
    DockerClient dockerClient = dbContainer.getDockerClient();
    String containerId = dbContainer.getContainerId();

    long start = System.currentTimeMillis();
    long timeout = 120000; // 120 seconds timeout
    String lastError = null;

    while (System.currentTimeMillis() - start < timeout) {
      try {
        // 1. Log real-time container status from Docker
        var state = dockerClient.inspectContainerCmd(containerId).exec().getState();
        boolean isRunning = Boolean.TRUE.equals(state.getRunning());
        Log.error(
            "Polling - Container Status: " + state.getStatus() + " (Running: " + isRunning + ")");

        if (isRunning) {
          // 2. Check internal Cassandra status via nodetool
          boolean isNodeUp = isCassandraUp(dockerClient, containerId);
          Log.error(
              "Polling - Cassandra Nodetool Status: " + (isNodeUp ? "UP" : "DOWN/Initializing"));
        }

        // 3. Perform a lightweight check (e.g., an empty find) to see if DB responds
        String json =
            """
              {
                "findOne": {}
              }
              """;

        var response =
            given()
                .headers(getHeaders())
                .contentType(ContentType.JSON)
                .body(json)
                .when()
                .post(CollectionResource.BASE_PATH, keyspaceName, collectionName);

        int statusCode = response.getStatusCode();

        // 200 OK means the DB handled the request (even if empty result)
        if (statusCode == 200) {
          Log.error("DB recovered! Received 200 OK.");
          return;
        } else {
          lastError = "Status: " + statusCode + ", Body: " + response.getBody().asString();
          Log.error("DB responded but not ready: " + lastError);
        }
      } catch (Exception e) {
        // Log connection errors
        lastError = "Exception: " + e.getMessage();
        Log.error("DB connection error during polling: " + lastError);
      }

      try {
        Thread.sleep(2000); // Poll every 2s to reduce log noise
      } catch (InterruptedException ignored) {
      }
    }
    throw new RuntimeException(
        "DB failed to recover within " + timeout + "ms. Last error: " + lastError);
  }

  /** Checks if Cassandra is up and normal by running "nodetool status" inside the container. */
  private boolean isCassandraUp(DockerClient dockerClient, String containerId) {
    try {
      var execCreateCmdResponse =
          dockerClient
              .execCreateCmd(containerId)
              .withAttachStdout(true)
              .withAttachStderr(true)
              .withCmd("nodetool", "status")
              .exec();

      var callback =
          new com.github.dockerjava.api.async.ResultCallback.Adapter<
              com.github.dockerjava.api.model.Frame>() {
            @Override
            public void onNext(com.github.dockerjava.api.model.Frame object) {
              // No-op: we don't strictly need to capture output here as we rely on exit code.
              // In a more advanced version, we could collect bytes here to check for "UN".
            }
          };

      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();

      // Note: In a real implementation we would capture stdout to check for "UN"
      // But WaitContainerResultCallback doesn't easily expose stdout capture without custom logic.
      // For now, if the command returns exit code 0, it means nodetool connected successfully,
      // which is a very strong indicator that JMX and the JVM are up.
      // A failed node usually causes nodetool to throw a ConnectException and exit with non-zero.

      // However, to be more robust for "UN" check, let's use a simpler approach:
      // If exit code is 0, we assume UP for debugging purposes.
      // (Capturing output requires a custom Adapter which adds complexity to this test class).

      // Let's rely on exit code for now.
      // 0 = Success (JMX connected, likely UP)
      // 1+ = Failed (JMX not ready)

      // We can improve this if needed by implementing a proper ResultCallback that collects bytes.

      // Actually, let's use a standard adapter to be sure:
      // But since we can't easily add inner classes, let's just use the exit code as a proxy.

      // Correction: WaitContainerResultCallback does not provide exit code directly in all
      // versions.
      // Let's use InspectExecCmd to get the exit code after execution.
      var inspectExecResponse = dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec();
      long exitCode = inspectExecResponse.getExitCodeLong();

      return exitCode == 0;
    } catch (Exception e) {
      Log.error("Failed to run nodetool status: " + e.getMessage());
      return false;
    }
  }
}
