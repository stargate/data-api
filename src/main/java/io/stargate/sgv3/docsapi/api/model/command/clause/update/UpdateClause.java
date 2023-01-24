package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv3.docsapi.api.model.command.deserializers.UpdateClauseDeserializer;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonDeserialize(using = UpdateClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example = """
             {"name": "Aaron", "country": "US"}
              """)
public record UpdateClause(List<UpdateOperation<?>> updateOperations) {}
