package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static org.hamcrest.Matchers.containsString;

import com.github.dockerjava.api.DockerClient;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class SessionEvictionIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Test
  public void testSessionEvictionOnAllNodesFailed() throws Exception {

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

    GenericContainer<?> dbContainer = StargateTestResource.getCassandraContainer();
    DockerClient dockerClient = dbContainer.getDockerClient();
    String containerId = dbContainer.getContainerId();
    Log.info("Pausing Database Container to simulate failure (Freeze)...");
    dockerClient.pauseContainerCmd(containerId).exec();

    // Should get AllNodeFailedException
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
        .body("$", responseIsErrorWithStatus())
        .body("errors[0].message", containsString("No node was available"));

    // restart container
    Log.info("Unpausing Database Container to simulate recovery...");
    dockerClient.unpauseContainerCmd(containerId).exec();

    // wait
    Log.info("start to sleep");
    Thread.sleep(30000);
    Log.info("end sleep");

    // restore
    //    createKeyspace();
    //    createDefaultCollection();

    // verify
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
}
