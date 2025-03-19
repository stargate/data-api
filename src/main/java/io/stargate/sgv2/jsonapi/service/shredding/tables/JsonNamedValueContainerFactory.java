package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;

/**
 * Creates a {@link JsonNamedValueContainer} from a {@link JsonNode} document, which could be a
 * document to insert or the contents of an update clause etc.
 *
 * <p>All we care about is that the top level fields in the document are turned into a {@link
 * JsonNamedValue}, th JSON value is convered from Jackson into a Java value via the {@link
 * JsonNodeDecoder}. AKA the RowShredder
 *
 * <p>Typically used with the {@link CqlNamedValueContainerFactory}
 */
public class JsonNamedValueContainerFactory {

  private final TableSchemaObject tableSchemaObject;
  private final JsonNodeDecoder jsonNodeDecoder;

  public JsonNamedValueContainerFactory(
      TableSchemaObject tableSchemaObject, JsonNodeDecoder jsonNodeDecoder) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
    this.jsonNodeDecoder =
        Objects.requireNonNull(jsonNodeDecoder, "jsonNodeDecoder must not be null");
  }

  /**
   * Shreds a document into the {@link JsonNamedValue}'s by extracting the Java value from the
   * Jackson document
   *
   * @param document the document to shred
   * @return A {@link JsonNamedValueContainer} of the values found in the document
   */
  public JsonNamedValueContainer create(JsonNode document) {

    var container = new JsonNamedValueContainer();
    document
        .fields()
        .forEachRemaining(
            entry -> {
              var namedValue =
                  new JsonNamedValue(
                      JsonPath.rootBuilder().property(entry.getKey()).build(), jsonNodeDecoder);
              if (namedValue.bind(tableSchemaObject)) {
                namedValue.prepare(entry.getValue());
              }
              // even if there was an error, we still want to put the named value into the container
              // so other code can see what was in the document
              container.put(namedValue);
            });
    return container;
  }

  public List<ParsedJsonDocument> create(List<JsonNode> documents) {

    List<ParsedJsonDocument> result = new ArrayList<>(documents.size());
    for (int i = 0; i < documents.size(); i++) {
      result.add(new ParsedJsonDocument(i, create(documents.get(i))));
    }
    return result;
  }

  public record ParsedJsonDocument(int offset, JsonNamedValueContainer namedValues)
      implements Recordable {

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("offset", offset).append("namedValues", namedValues);
    }
  }
}
