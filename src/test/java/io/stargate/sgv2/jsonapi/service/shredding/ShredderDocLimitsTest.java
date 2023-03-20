package io.stargate.sgv2.jsonapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ShredderDocLimitsTest {
  @Inject ObjectMapper objectMapper;

  @Inject Shredder shredder;

  @Inject DocumentLimitsConfig docLimits;

  @Nested
  class ValidationDocShapeViolations {
    @Test
    public void catchTooBigDoc() throws Exception {
      // Let's construct document above 1 meg limit (but otherwise legal), with
      // 100 x 10k String values, divided in 10 sub documents of 10 properties
      final ObjectNode bigDoc = objectMapper.createObjectNode();
      bigDoc.put("_id", 123);

      for (int ix1 = 0; ix1 < 10; ++ix1) {
        ObjectNode mainProp = bigDoc.putObject("prop" + ix1);
        for (int ix2 = 0; ix2 < 10; ++ix2) {
          mainProp.put("sub" + ix2, RandomStringUtils.randomAscii(10_000));
        }
      }

      Exception e = catchException(() -> shredder.shred(bigDoc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith("exceeds maximum allowed (" + docLimits.maxDocSize() + ")");
    }
  }
}
