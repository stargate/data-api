package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;

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
  public void testSessionEvictionOnAllNodesFailed() throws InterruptedException {

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
    if (dbContainer == null) {
      throw new RuntimeException("Cannot find Cassandra container.");
    }

    System.out.println("Stopping Database Container to simulate failure...");
    dbContainer.stop();

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
        .body("$", responseIsError());

    // restart container
    System.out.println("Starting Database Container to simulate recovery...");
    dbContainer.start();

    // wait
    Thread.sleep(20000);

    // restore
    createKeyspace();
    createDefaultCollection();

    // verify
    insertDoc(
        """
              {
                "insertOne": {
                  "document": {
                    "name": "after_crash"
                  }
                }
              }
              """);

    System.out.println("Test Passed: Session recovered after DB restart.");
  }
}
