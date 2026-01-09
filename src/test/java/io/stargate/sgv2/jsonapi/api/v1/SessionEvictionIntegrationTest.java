package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static org.hamcrest.Matchers.*;

import com.github.dockerjava.api.DockerClient;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.io.IOException;
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
   * database container. To ensure this test correctly interacts with (and pauses) its dedicated
   * container, we must retrieve the port directly from the isolated container instance.
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

    // 2. Pause/stop the container to simulate DB failure
    GenericContainer<?> dbContainer =
        SessionEvictionTestResource.getSessionEvictionCassandraContainer();
    DockerClient dockerClient = dbContainer.getDockerClient();
    String containerId = dbContainer.getContainerId();
    Log.info("Pausing Database Container to simulate failure (Freeze)...");
    dockerClient.pauseContainerCmd(containerId).exec();

    try {
      // 3. Verify failure: The application should receive a 500 error/AllNodeFailedException
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
          .statusCode(anyOf(is(500), is(504)))
          .body("$", responseIsErrorWithStatus());
      // .body("errors[0].message", containsString("AllNodesFailedException"));
      // .body("errors[0].message", containsString("No node was available"));

    } finally {
      // 4. Always unpause the container in finally block to ensure cleanup
      Log.info("Unpausing Database Container to simulate recovery...");
      dockerClient.unpauseContainerCmd(containerId).exec();
    }

    // 5. Wait for the database to become responsive again
    Log.info("start to sleep");
    Thread.sleep(30000);
    Log.info("end sleep");

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

  /** Pauses the container using either 'podman' or 'docker' command, depending on availability. */
  private void pauseContainer(String containerId) throws IOException, InterruptedException {
    if (isCommandAvailable("podman")) {
      runCommand("podman", "pause", containerId);
    } else if (isCommandAvailable("docker")) {
      runCommand("docker", "pause", containerId);
    } else {
      throw new RuntimeException("Neither 'podman' nor 'docker' command found to pause container.");
    }
  }

  /** Unpauses the container using either 'podman' or 'docker' command. */
  private void unpauseContainer(String containerId) throws IOException, InterruptedException {
    if (isCommandAvailable("podman")) {
      runCommand("podman", "unpause", containerId);
    } else if (isCommandAvailable("docker")) {
      runCommand("docker", "unpause", containerId);
    } else {
      // Best effort warning if unpause fails because no command is found
      System.err.println("WARNING: Could not unpause container, no container runtime found.");
    }
  }

  /** Checks if a shell command is available in the current environment. */
  private boolean isCommandAvailable(String command) {
    try {
      // Checking version is a safe, side-effect-free way to test existence
      new ProcessBuilder(command, "--version").start().waitFor();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /** Executes a shell command and waits for it to finish. */
  private void runCommand(String... command) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.inheritIO(); // Prints stdout/stderr to the console for debugging
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException(
          "Command failed with exit code " + exitCode + ": " + String.join(" ", command));
    }
  }

  /** Polls the database until it becomes responsive again. */
  private void waitForDbRecovery() {
    System.out.println("Waiting for DB to recover...");
    long start = System.currentTimeMillis();
    long timeout = 30000; // 30 seconds timeout

    while (System.currentTimeMillis() - start < timeout) {
      try {
        // Perform a lightweight check (e.g., an empty find) to see if DB responds
        String json =
            """
              {
                "find": {
                  "filter": {"name": "check_recovery"}
                }
              }
              """;

        int statusCode =
            io.restassured.RestAssured.given()
                .port(getTestPort())
                .headers(getHeaders())
                .contentType(io.restassured.http.ContentType.JSON)
                .body(json)
                .when()
                .post(GeneralResource.BASE_PATH + "/" + keyspaceName + "/" + collectionName)
                .getStatusCode();

        // 200 OK means the DB handled the request (even if empty result)
        if (statusCode == 200) {
          System.out.println("DB recovered!");
          return;
        }
      } catch (Exception e) {
        // Ignore connection errors and continue retrying
      }

      try {
        Thread.sleep(500); // Poll every 500ms
      } catch (InterruptedException ignored) {
      }
    }
    throw new RuntimeException("DB failed to recover within " + timeout + "ms");
  }
}
