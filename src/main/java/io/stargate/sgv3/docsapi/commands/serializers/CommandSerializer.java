package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CreateCollectionCommand;
import io.stargate.sgv3.docsapi.commands.FindCommand;
import io.stargate.sgv3.docsapi.commands.FindOneAndUpdateCommand;
import io.stargate.sgv3.docsapi.commands.FindOneCommand;
import io.stargate.sgv3.docsapi.commands.InsertOneCommand;
import io.stargate.sgv3.docsapi.commands.UpdateOneCommand;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause;
import io.stargate.sgv3.docsapi.commands.clauses.update.SetUpdateOperation;
import java.io.IOException;
import javax.enterprise.context.ApplicationScoped;

/**
 * Encapsulates the setup for Jackson to deserialise and serialise commands to JSON
 *
 * <p>NOTE: Must register mixins and deserializers (see below) in the maps so they can be found!
 *
 * <p>Cache one instance, thread safe.
 *
 * <p>We keep the serialisation instructions out of the command / clause hierarchy so that model can
 * be simple and perhaps used later for clients. More importantly so that serialisation bugs,
 * changes, or whole new approaches (e.g. creating from protobuf) can be done without changing the
 * command classes.
 *
 * <p>Approach:
 *
 * <p>1. Define a mixin for each command subclass and add the command subclass to {@link
 * CommandMixin}
 *
 * <p>see https://www.baeldung.com/jackson-inheritance
 *
 * <p>2. In the mixin for the subclass define the command name in JSON and the signature of the
 * constructor to use to create the class. Note, this ctor will not be called but Jackson uses the
 * annotations to work out what data to inject.
 *
 * <p>See https://dzone.com/articles/jackson-mixin-to-the-rescue
 *
 * <p>3. Where we need to "re-map" data types, e.g. take a map of things into a list of things we
 * use custom deserializers. e.g. there is a map of {"cmd1" : {}, "cmd2": {} } and we want a list of
 * the commands. Or there is a map and we just want to get the map out. Examples of their use in
 * {@link UpdateClauseMixin} and {@link SetUpdateOperationMixin}
 *
 * <p>See https://www.baeldung.com/jackson-deserialization
 *
 * <p>TODO TO BE REPLACED - Deserializer used for the {@link FilterClauseDeserializer}, this was
 * first code and should be killed
 */
@ApplicationScoped
public class CommandSerializer {

  private final ObjectMapper mapper;

  public CommandSerializer() {

    mapper =
        JsonMapper.builder()
            .enable(
                MapperFeature
                    .ACCEPT_CASE_INSENSITIVE_ENUMS) // so "before" will match to "BEFORE" in an enum
            .build();

    // Command level config here
    mapper
        .addMixIn(Command.class, CommandMixin.class)
        .addMixIn(FindCommand.class, FindCommandMixin.class)
        .addMixIn(FindOneCommand.class, FindOneCommandMixin.class)
        .addMixIn(InsertOneCommand.class, InsertOneCommandMixin.class)
        .addMixIn(UpdateOneCommand.class, UpdateOneCommandMixin.class)
        .addMixIn(FindOneAndUpdateCommand.class, FindOneAndUpdateCommandMixin.class)
        .addMixIn(
            FindOneAndUpdateCommand.Options.class, FindOneAndUpdateCommandMixin.OptionsMixin.class)
        .addMixIn(CreateCollectionCommand.class, CreateCollectionCommandMixin.class);

    // Update Clause config here
    mapper
        .addMixIn(UpdateClause.class, UpdateClauseMixin.class)
        .addMixIn(SetUpdateOperation.class, SetUpdateOperationMixin.class);

    // Filter Clause - TODO maybe this can be done like the update clause
    SimpleModule module = new SimpleModule();
    module.addDeserializer(FilterClause.class, new FilterClauseDeserializer());

    mapper.registerModule(module);
  }

  public Command deserialize(String node) throws IOException {
    Command command = mapper.readValue(node, Command.class);
    return command;
  }
}
