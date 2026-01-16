package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static org.hamcrest.Matchers.*;

import com.github.dockerjava.api.DockerClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
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
  private static final Logger logger =
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
     * Overridden to enforce a fixed port binding for the Cassandra container.
     *
     * <p>Standard Testcontainers use random port mapping. However, this test manually stops and
     * restarts the container to simulate failure. In some environments, a restarted container might
     * not retain its original random port mapping, or the port forwarding may break.
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
        container.setPortBindings(Collections.singletonList(port + ":9042"));
      } catch (java.io.IOException e) {
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
    // Use low-level dockerClient to stop the container without triggering Testcontainers'
    // cleanup/termination logic (which dbContainer.stop() would do).
    // This effectively "pulls the plug" while keeping the container instance intact for restart.
    dockerClient.stopContainerCmd(containerId).exec();

    // 3. Verify failure: The request should receive a 500 error/AllNodesFailedException
    givenHeadersPostJsonThen(
            """
              {
                "findOne": {}
              }
              """)
        .statusCode(500)
        .body("errors[0].message", containsString("No node was available"));

    // 4. Restart the container to simulate recovery
    dockerClient.startContainerCmd(containerId).exec();

    // 5. Wait for the database to become responsive again
    waitForDbRecovery();

    // 6. Verify Session Recovery: insert and find data again, the request should succeed
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
   * Polls the Data API until it returns a successful response, indicating the database has
   * recovered.
   *
   * @throws RuntimeException if the system does not recover within the timeout period.
   */
  private void waitForDbRecovery() {
    logger.info("Waiting for DB to recover...");
    GenericContainer<?> dbContainer =
        SessionEvictionTestResource.getSessionEvictionCassandraContainer();
    DockerClient dockerClient = dbContainer.getDockerClient();
    String containerId = dbContainer.getContainerId();
    long start = System.currentTimeMillis();
    long timeout = 120000; // 120 seconds timeout
    String lastError = null;
    while (System.currentTimeMillis() - start < timeout) {
      try {
        var inspect = dockerClient.inspectContainerCmd(containerId).exec();
        var state = inspect.getState();
        boolean isRunning = Boolean.TRUE.equals(state.getRunning());
        var ports = inspect.getNetworkSettings().getPorts().getBindings();
        int mappedPort = dbContainer.getMappedPort(9042);
        logger.info(
            "Polling - Container Status: "
                + state.getStatus()
                + " (Running: "
                + isRunning
                + "), Ports: "
                + ports
                + ", CurrentMappedPort: "
                + mappedPort);

        // 2. TCP Socket Probe
        try (java.net.Socket socket = new java.net.Socket()) {
          socket.connect(new java.net.InetSocketAddress("localhost", mappedPort), 2000);
          logger.info("Polling - Socket Probe: SUCCESS (TCP Handshake OK)");
        } catch (Exception e) {
          logger.info("Polling - Socket Probe: FAILED - " + e.getMessage());
        }

        if (!isRunning) {
          logger.info("CRITICAL: Container is NOT running. Performing post-mortem...");
          logger.info("  Exit Code: " + state.getExitCode()); // 137 = OOM Killed
          logger.info("  OOMKilled: " + state.getOOMKilled());
          try {
            logger.info("--- CONTAINER LOGS (LAST 50 LINES) ---");
            dockerClient
                .logContainerCmd(containerId)
                .withStdOut(true)
                .withStdErr(true)
                .withTail(50)
                .exec(
                    new com.github.dockerjava.api.async.ResultCallback.Adapter<
                        com.github.dockerjava.api.model.Frame>() {
                      @Override
                      public void onNext(com.github.dockerjava.api.model.Frame frame) {
                        logger.info(new String(frame.getPayload()));
                      }
                    })
                .awaitCompletion();
            logger.info("--- END CONTAINER LOGS ---");
          } catch (Exception logEx) {
            logger.info("Failed to fetch container logs: " + logEx.getMessage());
          }
        }
        if (isRunning) {
          boolean isNodeUp = isCassandraUp(dockerClient, containerId);
          boolean isNativeTransportActive = isNativeTransportActive(dockerClient, containerId);
          logger.info(
              "Polling - Cassandra Status: NodeUp="
                  + isNodeUp
                  + ", NativeTransport="
                  + isNativeTransportActive);
        }
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
        if (statusCode == 200) {
          logger.info("DB recovered! Received 200 OK.");
          return;
        } else {
          lastError = "Status: " + statusCode + ", Body: " + response.getBody().asString();
          logger.info("DB responded but not ready: " + lastError);
        }
      } catch (Exception e) {
        lastError = "Exception: " + e.getMessage();
        logger.info("DB connection error during polling: " + lastError);
      }
      try {
        Thread.sleep(2000); // Poll every 2s
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
            public void onNext(com.github.dockerjava.api.model.Frame object) {}
          };

      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
      var inspectExecResponse = dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec();
      return inspectExecResponse.getExitCodeLong() == 0;
    } catch (Exception e) {
      logger.info("Failed to run nodetool status: " + e.getMessage());
      return false;
    }
  }

  /** Checks if Native Transport is active by running "nodetool info" inside the container. */
  private boolean isNativeTransportActive(DockerClient dockerClient, String containerId) {
    try {
      var execCreateCmdResponse =
          dockerClient
              .execCreateCmd(containerId)
              .withAttachStdout(true)
              .withAttachStderr(true)
              .withCmd("nodetool", "info")
              .exec();

      StringBuilder output = new StringBuilder();
      var callback =
          new com.github.dockerjava.api.async.ResultCallback.Adapter<
              com.github.dockerjava.api.model.Frame>() {
            @Override
            public void onNext(com.github.dockerjava.api.model.Frame frame) {
              output.append(new String(frame.getPayload()));
            }
          };

      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
      return output.toString().contains("Native Transport active: true");
    } catch (Exception e) {
      logger.info("Failed to run nodetool info: " + e.getMessage());
      return false;
    }
  }
}
