package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

/**
 * Jackson {@link com.fasterxml.jackson.databind.JsonSerializer} for {@link CqlVector}: needed
 * mostly to serializer vector if it is part of primary for Insert operation (for Find command
 * handled with different codec that does CQL to {@code JsonNode} conversion).
 */
public class CqlVectorSerializer extends StdSerializer<CqlVector<Float>> {
  public CqlVectorSerializer() {
    super(CqlVector.class, true);
  }

  @Override
  public void serialize(CqlVector<Float> vector, JsonGenerator g, SerializerProvider ctxt)
      throws IOException {
    g.writeStartArray(vector, vector.size());
    for (Float f : vector) {
      g.writeNumber(f);
    }
    g.writeEndArray();
  }
}
