package io.stargate.sgv3.docsapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv3.docsapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.InsertOneCommand;

/**
 * POJO object (data no behavior) that represents a syntactically and grammatically valid command as
 * defined in the API spec.
 *
 * <p>The behavior about *how* to run a Command is in the {@link
 * io.stargate.sgv3.docsapi.service.resolver.model.CommandResolver}.
 *
 * <p>Commands <b>should not</b> include JSON other than for documents we want to insert. They
 * should represent a translate of the API request into an internal representation. e.g. this
 * insulates from tweaking JSON on the wire protocol, we would only need to modify how we create the
 * command and nothing else.
 *
 * <p>These may be created from parsing the incoming message and could also be created
 * programmatically for internal and testing purposes.
 *
 * <p>Each command should validate itself using the <i>javax.validation</i> framework.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.WRAPPER_OBJECT,
    property = "commandName")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CreateCollectionCommand.class),
  @JsonSubTypes.Type(value = FindOneCommand.class),
  @JsonSubTypes.Type(value = InsertOneCommand.class),
})
public interface Command {}
