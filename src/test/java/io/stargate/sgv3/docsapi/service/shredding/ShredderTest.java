package io.stargate.sgv3.docsapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ShredderTest {
  @Inject ObjectMapper objectMapper;

  private final Shredder shredder = new Shredder();

  @Test
  public void simpleShredFromPathExample() throws Exception {
    final String JSON =
        """
{ "_id" : "abc",
  "name" : "Bob",
  "values" : [ 1, 2 ],
  "[extra.stuff]" : true
}
                """;
    WritableShreddedDocument doc = shredder.shred(objectMapper.readTree(JSON));
    assertThat(doc.id()).isEqualTo("abc");
    List<JSONPath> expPaths =
        Arrays.asList(
            JSONPath.fromEncoded("name"),
            JSONPath.fromEncoded("values"),
            JSONPath.fromEncoded("values.[0]"),
            JSONPath.fromEncoded("values.[1]"),
            JSONPath.fromEncoded("\\[extra\\.stuff]"));

    assertThat(doc.docFieldOrder()).isEqualTo(expPaths);
    assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
  }

  @Test
  public void docBadJSONType() throws Exception {
    final String JSON = "[ 1, 2 ]";
    Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree(JSON)));
    assertThat(t)
        .isNotNull()
        .hasMessageContaining("Document to shred must be a JSON Object: instead got ARRAY");
  }

  @Test
  public void docBadDocIdType() {
    final String JSON = """
{ "_id" : [ ] }
                """;
    Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree(JSON)));
    assertThat(t).isNotNull().hasMessageContaining("Bad type for '_id' property (ARRAY)");
  }
}
