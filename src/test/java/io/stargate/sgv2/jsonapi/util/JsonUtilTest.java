package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.io.IOException;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class JsonUtilTest {
  @Inject ObjectMapper mapper;

  @Nested
  class EqualityTests {
    @Test
    public void testScalarOrderedEquality() {
      assertThat(equalsOrdered("1", "1")).isTrue();
      assertThat(equalsOrdered("true", "true")).isTrue();
      assertThat(equalsOrdered("\"text\"", "\"text\"")).isTrue();

      assertThat(equalsOrdered("1", "2")).isFalse();
      assertThat(equalsOrdered("true", "false")).isFalse();
      assertThat(equalsOrdered("\"abc\"", "\"bcd\"")).isFalse();

      assertThat(equalsOrdered("0", "true")).isFalse();
    }

    @Test
    public void testArrayOrderedEquality() {
      assertThat(equalsOrdered("[ 1 ]", "[ 1 ]")).isTrue();

      assertThat(equalsOrdered("[1, 2]", "[2, 1]")).isFalse();
      assertThat(equalsOrdered("[1, 2]", "[1]")).isFalse();
    }

    @Test
    public void testObjectOrderedEquality() {
      assertThat(equalsOrdered("{ \"a\":1, \"b\":2 }", "{ \"a\":1, \"b\":2 }")).isTrue();
      assertThat(equalsOrdered("[{ \"a\":1, \"b\":2 }]", "[{ \"a\":1, \"b\":2 }]")).isTrue();
      assertThat(
              equalsOrdered(
                  "{ \"subdoc\" : { \"x\":0, \"y\":5 } }", "{ \"subdoc\" : { \"x\":0, \"y\":5 } }"))
          .isTrue();

      // The only interesting test; that for Objects: order of property must match too
      assertThat(equalsOrdered("{ \"a\":1, \"b\":2 }", "{ \"b\":2, \"a\":1 }")).isFalse();
      assertThat(equalsOrdered("[{ \"a\":1, \"b\":2 }]", "[{ \"b\":2, \"a\":1 }]")).isFalse();
      assertThat(
              equalsOrdered(
                  "{ \"subdoc\" : { \"x\":0 } }", "{ \"subdoc\" : { \"x\":0, \"y\":5 } }"))
          .isFalse();
      assertThat(
              equalsOrdered(
                  "{ \"subdoc\" : { \"y\":5, \"x\":0 } }", "{ \"subdoc\" : { \"x\":0, \"y\":5 } }"))
          .isFalse();
    }

    boolean equalsOrdered(String json1, String json2) {
      try {
        return JsonUtil.equalsOrdered(mapper.readTree(json1), mapper.readTree(json2));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
