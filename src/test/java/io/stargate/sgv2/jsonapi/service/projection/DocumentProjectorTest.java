package io.stargate.sgv2.jsonapi.service.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocumentProjectorTest {
  @Inject ObjectMapper objectMapper;

  // Tests for validating issues with Projection definitions
  @Nested
  class ProjectorDefValidation {
    @Test
    public void verifyProjectionJsonObject() throws Exception {
      JsonNode def = objectMapper.readTree(" [ 1, 2, 3 ]");
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage("Unsupported projection parameter: definition must be OBJECT, was ARRAY");
    }

    @Test
    public void verifyProjectionEquality() throws Exception {
      String defStr1 = "{ \"field1\" : 1, \"field2\": 0 }";
      String defStr2 = "{ \"field1\" : 0, \"field2\": 1 }";

      DocumentProjector proj1 =
          DocumentProjector.createFromDefinition(objectMapper.readTree(defStr1));
      DocumentProjector proj2 =
          DocumentProjector.createFromDefinition(objectMapper.readTree(defStr2));

      // First, verify equality of identical definitions
      assertThat(proj1)
          .isEqualTo(DocumentProjector.createFromDefinition(objectMapper.readTree(defStr1)));
      assertThat(proj2)
          .isEqualTo(DocumentProjector.createFromDefinition(objectMapper.readTree(defStr2)));

      // TODO: inequality
    }
  }
}
