package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.github.dockerjava.api.DockerClient;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.IsolatedDseTestResource;
import io.stargate.sgv2.jsonapi.testresource.StargateTestResource;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

@QuarkusIntegrationTest
@QuarkusTestResource(IsolatedDseTestResource.class)
public class SessionEvictionIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Override
  protected synchronized CqlSession createDriverSession() {
    if (cqlSession == null) {
      GenericContainer<?> container = IsolatedDseTestResource.getIsolatedContainer();
      if (container == null) {
        throw new IllegalStateException("Isolated container not started!");
      }
      int port = container.getMappedPort(9042);
      String dc;
      if (StargateTestResource.isDse() || StargateTestResource.isHcd()) {
        dc = "dc1";
      } else {
        dc = "datacenter1";
      }
      var builder =
          new CqlSessionBuilder()
              .withLocalDatacenter(dc)
              .addContactPoint(new InetSocketAddress("localhost", port))
              .withAuthCredentials("cassandra", "cassandra"); // default admin password :)
      cqlSession = builder.build();
    }
    return cqlSession;
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
    GenericContainer<?> dbContainer = IsolatedDseTestResource.getIsolatedContainer();
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
          .statusCode(500)
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
