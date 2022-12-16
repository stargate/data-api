package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv3.docsapi.commands.CreateCollectionCommand;
import io.stargate.sgv3.docsapi.commands.FindCommand;
import io.stargate.sgv3.docsapi.commands.FindOneAndUpdateCommand;
import io.stargate.sgv3.docsapi.commands.FindOneCommand;
import io.stargate.sgv3.docsapi.commands.InsertOneCommand;
import io.stargate.sgv3.docsapi.commands.UpdateOneCommand;

/**
 * The mixin for the {@link Command} super class.
 *
 * <p>The {@link JsonTypeInfo} tells jackson there is a single property in the JSON object that
 * defines the type of the subclass. We have called that property {@code commandName}, but it does
 * not have to exist on the objects we are creating. So use {@link JsonIgnoreProperties} to tell
 * Jackson to ignore it. The COmmand subclasses know what they are, they don't need a property that
 * jackson would not call.
 *
 * <p>The {@link JsonSubTypes} lists the subclasses, the Mixin for these use the {@link
 * JsonTypeName} to say what value of "commandName" to map to them. See {@link FindOneCommandMixin}
 *
 * <p>With this in place we can ask Jackson to deserialise an incoming message to a Command and we
 * get a subclass.
 *
 * <p>see -
 * https://stackoverflow.com/questions/58260217/deserialize-wrapper-object-key-as-property-using-jackson
 * - https://www.baeldung.com/jackson-inheritance
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.WRAPPER_OBJECT,
    property = "commandName")
@JsonSubTypes({
  @JsonSubTypes.Type(FindCommand.class),
  @JsonSubTypes.Type(FindOneCommand.class),
  @JsonSubTypes.Type(InsertOneCommand.class),
  @JsonSubTypes.Type(UpdateOneCommand.class),
  @JsonSubTypes.Type(FindOneAndUpdateCommand.class),
  @JsonSubTypes.Type(CreateCollectionCommand.class)
})
@JsonIgnoreProperties({"commandName"})
public abstract class CommandMixin {}
