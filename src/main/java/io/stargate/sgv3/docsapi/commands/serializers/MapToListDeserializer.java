package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes a JSON map of {"cmd1" : {}, "cmd2": {} } , where cmd's are polymorphic and
 * creates a list of configured commands.
 *
 * This is roughly expanding the features in {@link JsonTypeInfo.As.WRAPPER_OBJECT} so the
 * "type" information is not a single field in the object but every key.
 *
 * For example, the update clause could be:
 *
 * <pre>
 * "update" : {"$set" : {"updated_field" : "new value"}, "$unset" : {"old field" : ""}}
 * </pre>
 *
 * $set and $unset are two subclasses of the {@link UpdateClauseOperation} but there
 * are more, and we want to get a list of all the update operations.
 *
 * So the {@link UpdateClauseMixin} is configured as:
 *
 * <pre>
 *  @JsonCreator(mode= Mode.DELEGATING)
 *   public UpdateClauseMixin(
 *       @JsonDeserialize(using = UpdateClauseOperationListDeserializer.class)  List<UpdateClauseOperation> operations) {
 *
 *       throw new UnsupportedOperationException();
 *   }
 *
 * With {@link UpdateClauseOperationListDeserializer} subclasses this to handle the creation of the
 * operations. Jackson uses our deserializer and calls the ctor for {@link UpdateClause} with a
 * list of {@code List<UpdateClauseOperation}
 *
 * Subclasses create the objects in {@link #create(DeserializationContext, String, JsonNode)} and
 * should use the {@link DeserializationContext} to do so. This will have all the config from the
 * mapper created in {@link CommandSerializer}. e.g.
 *
 * <pre>
 * protected UpdateClauseOperation create(DeserializationContext ctxt, String key, JsonNode value) throws IOException{
 * 		switch (key){
 *            case "$set" :
 *                return ctxt.readTreeAsValue(value, SetUpdateOperation.class);
 *            default:
 *                return null;
 *        }
 * 	}
 * </pre>
 *
 * T - type for the list to contain, subclasses create the objects so T can be the superclass
 *      of objects for the list.
 */
public abstract class MapToListDeserializer<T> extends StdDeserializer<List<T>> {

  public MapToListDeserializer() {
    this(null);
  }

  public MapToListDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public List<T> deserialize(JsonParser parser, DeserializationContext ctxt)
      throws IOException, JacksonException {

    List<T> items = new ArrayList<>();

    JsonNode mapObject = ctxt.readTree(parser);
    var iter = mapObject.fields();

    while (iter.hasNext()) {
      var entry = iter.next();

      // first token is the field name, our "type" information
      String opName = entry.getKey();
      // next token is the object / operation that has the data for the object to create
      var opConfig = entry.getValue();
      items.add(create(ctxt, opName, opConfig));
    }
    return items;
  }

  protected abstract T create(DeserializationContext ctxt, String key, JsonNode value)
      throws IOException;
}
