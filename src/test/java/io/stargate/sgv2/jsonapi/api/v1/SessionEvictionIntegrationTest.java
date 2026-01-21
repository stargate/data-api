package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static org.hamcrest.Matchers.*;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = SessionEvictionIntegrationTest.SessionEvictionTestResource.class,
    restrictToAnnotatedClass = true)
public class SessionEvictionIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SessionEvictionIntegrationTest.class);

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
     * Overridden to enforce a fixed port binding for the Cassandra container native binary / CQL
     * port (9042).
     *
     * <p>Standard Testcontainers use random port mapping. However, this test manually stops and
     * restarts the container to simulate failure. Under normal circumstances, a restarted container
     * will not retain its original random port mapping, causing the initial port forwarding to
     * break.
     *
     * <p>By using a fixed port binding (finding an available local port and mapping it explicitly),
     * we ensure the database is always accessible on the same port after a restart, allowing the
     * Java driver to successfully reconnect.
     */
    @Override
    protected GenericContainer<?> baseCassandraContainer(boolean reuse) {
      GenericContainer<?> container = super.baseCassandraContainer(reuse);
      try (ServerSocket socket = new ServerSocket(0)) {
        int port = socket.getLocalPort();
        // Map the randomly selected available host port to the container's native CQL port (9042)
        container.setPortBindings(Collections.singletonList(port + ":9042"));
      } catch (IOException e) {
        throw new RuntimeException("Failed to find open port", e);
      }
      return container;
    }

    /**
     * Starts the container and captures the reference.
     *
     * <p>We override this method to capture the container instance created by the superclass into
     * our static {@link #sessionEvictionCassandraContainer} field, making it accessible to the test
     * methods.
     */
    @Override
    public Map<String, String> start() {
      var props = super.start();
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
    protected void setSystemProperties(Map<String, String> props) {
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

  /**
   * @return The DockerClient for the isolated Cassandra container.
   */
  private DockerClient getDockerClient() {
    return SessionEvictionTestResource.getSessionEvictionCassandraContainer().getDockerClient();
  }

  /**
   * @return The container ID of the isolated Cassandra container.
   */
  private String getContainerId() {
    return SessionEvictionTestResource.getSessionEvictionCassandraContainer().getContainerId();
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
    // Use low-level dockerClient to stop the container without triggering Testcontainers'
    // cleanup/termination logic (which dbContainer.stop() would do).
    // This effectively "pulls the plug" while keeping the container instance intact for restart.
    getDockerClient().stopContainerCmd(getContainerId()).exec();

    // 3. Verify failure: The request should receive a 500 error/AllNodesFailedException
    givenHeadersPostJsonThen(
            """
              {
                "findOne": {}
              }
              """)
        .statusCode(200)
        .body(
            "errors[0].errorCode", is(DatabaseException.Code.FAILED_TO_CONNECT_TO_DATABASE.name()));

    // 4. Restart the container to simulate recovery
    getDockerClient().startContainerCmd(getContainerId()).exec();

    // 5. Wait for the database to become responsive again
    waitForDbRecovery();

    // 6. Verify Session Recovery: check the data before crashing
    // Not to check that cassandra works, but to check that we are running the same container as
    // before. We want to verify that the data / state before the crash is the same as after.
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

    // 7. Verify Session Recovery: insert and find data again, the request should succeed
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
  }

  /**
   * Polls the DB container until the container is running, Cassandra is up, and the API request
   * returns 200.
   *
   * @throws RuntimeException if recovery does not occur within the timeout period.
   */
  private void waitForDbRecovery() {
    long start = System.currentTimeMillis();
    long timeout = 120000; // 120 seconds timeout
    boolean isContainerRunning = false;
    boolean isCassandraUp = false;
    boolean isAPIReady = false;

    while (System.currentTimeMillis() - start < timeout) {

      // 1. Check container
      isContainerRunning = isContainerRunning();

      // 2. Check Cassandra (only after the container is running)
      isCassandraUp = isContainerRunning && isCassandraUp(getDockerClient(), getContainerId());

      // 3. Check API (only after Cassandra is up)
      isAPIReady = isCassandraUp && isApiReady();
      if (isAPIReady) {
        LOGGER.info(
            "Database and API have recovered after {} ms.", (System.currentTimeMillis() - start));
        return;
      }

      // Poll every 1s
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    throw new RuntimeException(
        "DB failed to recover within "
            + timeout
            + "ms. Container status: "
            + isContainerRunning
            + ", Cassandra status: "
            + isCassandraUp
            + ", API status: "
            + isAPIReady);
  }

  /** Checks if the Cassandra container is currently in the "running" state. */
  private boolean isContainerRunning() {
    return Boolean.TRUE.equals(
        getDockerClient().inspectContainerCmd(getContainerId()).exec().getState().getRunning());
  }

  /**
   * Checks if the API is responsive and returning a success response.
   *
   * @return true if the API returns 200 OK and the body indicates success (no errors, has data).
   */
  private boolean isApiReady() {
    try {
      Response response =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body("{\"findOne\": {}}")
              .post(CollectionResource.BASE_PATH, keyspaceName, collectionName);

      if (response.statusCode() != 200) {
        return false;
      }

      // Success means: has "data", no "errors", no "status" (for findOne)
      var jsonPath = response.jsonPath();
      return jsonPath.get("data") != null
          && jsonPath.get("errors") == null
          && jsonPath.get("status") == null;

    } catch (Exception e) {
      LOGGER.warn("Error checking API status: {}", e.getMessage(), e);
      return false;
    }
  }

  /** Checks if Cassandra is up and normal by running "nodetool status" inside the container. */
  private boolean isCassandraUp(DockerClient dockerClient, String containerId) {
    try {
      // .exec() here merely registers the command with Docker and returns an execution ID. It does
      // NOT start the command yet.
      var execCreateCmdResponse =
          dockerClient
              .execCreateCmd(containerId)
              .withAttachStdout(true)
              .withAttachStderr(true)
              .withCmd("nodetool", "status")
              .exec();

      // Capture the output (stdout/stderr) from the container command
      // The Docker Java API for 'execStart' is asynchronous and requires a ResultCallback to handle
      // the stream of output (Frames) from the container.
      StringBuilder output = new StringBuilder();
      var callback =
          new ResultCallback.Adapter<Frame>() {
            @Override
            public void onNext(Frame object) {
              output.append(new String(object.getPayload()));
            }
          };

      // .exec(callback) triggers the actual execution in the container.
      // .awaitCompletion() blocks until the command finished or the callback is closed.
      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
      var inspectExecResponse = dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec();

      // Check that the command succeeded (exit code 0) AND the node status implies "UN" (Up/Normal)
      return inspectExecResponse.getExitCodeLong() == 0 && output.toString().contains("UN");
    } catch (Exception e) {
      LOGGER.warn("Error checking Cassandra status: {}", e.getMessage(), e);
      return false;
    }
  }
}
