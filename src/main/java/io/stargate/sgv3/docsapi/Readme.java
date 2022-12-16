package io.stargate.sgv3.docsapi;

/**
 * NOTE: This class is just here so I can write a readme for the architecture the uses links to
 * code.
 *
 * <p>The design tries to:
 *
 * <p>- Support multiple developer work streams that can work against interfaces or mocks. - Run
 * from day 0 with very limited functionality, that can be expanded with parallel developer effort.
 * - Run from day 0 with the DB features we have, and then take advantage of the things later such
 * as SAI improvements. - Progressively support more complex query patterns.
 *
 * <p>The architecture from top to bottom is below, in general think of each tier of being able to
 * be tested with mocks above and below it. Quick hint, Commands are things defined in the public
 * API like findOne, and Operations are what we do with Cassandra to get the data in and out of the
 * DB.
 *
 * <p><strong>Serving Tier</strong>
 *
 * <p>The components that make a HTTP interface available, interact with the rest of Stargate, and
 * host the API engine in this project. The Serving Tier is not in this project, think of it as a
 * host for this engine and that we may want to host it another way in the future such as gRPC.
 *
 * <p>Above: Public HTTP Below: Engine Tier, sends data objects {@link
 * docapi.messages.RequestMessage} to {@link docapi.DocAPIEngine}
 *
 * <p>The Serving tier:
 *
 * <p>- Provides the HTTP interface according to the HTTP spec. - Understands hosting, start up,
 * tear down, ready state, everything to be a good Stargate service. - Hosts the API engine
 * represented by {@link docapi.DocAPIEngine} - For each request translates the HTTP to and from
 * {@link docapi.messages.Message} and builds the {@link CommandContext} - Calls the API engine with
 * the requests.
 *
 * <p><strong>Engine Tier</strong>
 *
 * <p>The entry point to get the API engine to do things, encapsulates all handling of requests.
 * Really this is just the {@link docapi.DocAPIEngine}.
 *
 * <p>Above: Serving Tier, returns data objects {@link docapi.messages.ResponseMessage} Below:
 * Command Tier, sends data objects {@link docapi.commands.Command} to {@link
 * docapi.service.CommandProcessor}
 *
 * <p>The Engine tier:
 *
 * <p>- Encapsulates all processing of requests, you can spin it up and run it outside of the
 * Serving tier. - Accepts and returns JSON messages that follow the API spec via {@link
 * docapi.messages.Message} - Manages translating JSON in the {@link docapi.commands.Command} 's
 * that provide an internal (non JSON) model of the request - Dispatches {@link
 * docapi.commands.Command} to be processed without knowing how that happens.
 *
 * <p><strong>Command Tier</strong>
 *
 * <p>The internal representation of the API specification, i.e. this is where a findOne API command
 * is located and the knowledge about how parse it and then what requests to make to the DB, but now
 * *how* to do it. This is also where shredding happens.
 *
 * <p>Above: Engine Tier, returns data objects {@link docapi.commands.CommandResult} Below:
 * Operation Tier, sends objects with behavior {@link docapi.operations.Operation} and data objects
 * {@link docapi.shredding.WritableShreddedDocument} to {@link docapi.service.OperationExecutor}
 *
 * <p>The Command tier:
 *
 * <p>- Provides a programmatic way to describe all public API commands via data objects under
 * {@link docapi.commands.Command} which includes the clauses, the most complex of which is {@link
 * docapi.commands.clauses.FilterClause} - Encapsulates the running of any valid {@link
 * docapi.commands.Command} and does it without needing anything from the Serving tier. - Defines
 * the behavior of how to parse a command and work out the DB {@link docapi.operations.Operation}
 * using {@link docapi.commands.resolvers.CommandResolver}'s which so the logic is separated from
 * the command definition. - Uses pattern matching to parse the Filter clauses, that will hopefully
 * be expandable, e.g. {@link docapi.commands..resolvers.FindOneCommandResolver} - Uses {@link
 * docapi.shredding.Shredder} to translate JSON documents to/ from the internal {@link
 * docapi.shredding.WritableShreddedDocument} model that insulated the operations tier from JSON. -
 * Dispatches the {@link docapi.operations.Operation} to be processed without knowing how that
 * happens.
 *
 * <p><strong>Operation Tier</strong>
 *
 * <p>The actual Cassandra DP operations we want to run against Cassandra which are isolated from
 * the API requests and JSON processing. Operation tier knows how to do simple pushdown to DB
 * operations like fetch by document ID, and will also encapsulate operations that cannot be pushed
 * down such as client side filtering or sorting. Operations tier never needs to worry about JSON,
 * and the higher tiers never need to know about the DB schema.
 *
 * <p>Above: Command Tier, returns data objects {@link docapi.operations.OperationResult} Below: C*
 * DB, sends CQL statements.
 *
 * <p>The Operation tier:
 *
 * <p>- Encapsulates all DB operations, if we change the way we connect to the DB or the schema it
 * does not impact higher tiers. - Provides specific, tunable, and testable way that get / set data
 * in the DB that can be run created and run on its own such as {@link
 * docapi.operations.FindByIdOperation} or {@link docapi.operations.FindOneByOneTextOperation} -
 * Models documents using the {@link docapi.shredding.WritableShreddedDocument} classes only. -
 * Allows for operations to be run with the resources they need, e.g. fast operations in one pool
 * longer in another. - Allows for very specific operations and very general, e.g. find by one text
 * field and a more general and complex find by anything - Manages the DB connection and is tenant
 * aware. - Uses the tenant, auth, db name, and collection / table name in {@link
 * docapi.commands.CommandContext} to send the db ops to the correct place.
 */
public class Readme {}
