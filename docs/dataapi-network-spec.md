# JSON HTTP API Specification

This document specifies the HTTP API for the DATA API. That is how clients can communicate with the service. 
See the [DATA API Query Specification](dataapi-spec.md) for details of data modelling and queries.

- [Preamble](#preamble)
- [High-level concepts](#high-level-concepts)
- [Conventions](#conventions)
- [Error handling](#error-handling)
- [Endpoints](#endpoints)
    - [Namespace endpoint](#namespace-endpoint)
        - [Namespace endpoint errors](#errors)
    - [Collection endpoint](#collection-endpoint)
- [Authentication and Authorization](#authentication-and-authorization)

## Preamble

The target users for the DATA API are Javascript developers who interact
with the service through a driver or Object Document Mapper (ODM)
library such as [Mongoose](https://github.com/Automattic/mongoose). We
expect both ODMs and developers not using an ODM to use a client library
that manages connections and encoding, and provides a basic idiomatic
representation of the DATA API. We do not expect developers to construct
and make raw HTTP requests against the DATA API. With that in mind the
HTTP API is designed to be processed by a client-side driver first, and
humans second.

The HTTP API has the following design principles:

1.  **Message-Based:** With the exclusion of the endpoint address and
    client security context, the entire request from a client and
    response from the server **should** be encapsulated in a single
    message in the body of the request. As an illustration, it should be
    possible to easy capture, store, and later send a request message to
    a different endpoint and compare the response message to the
    original response.
2.  **Always Respond:** The DATA API **should** always send a response
    message, even in the case of an error, this is a corollary to
    "Message Based". This means that errors in processing a request,
    such as authentication errors or a timeout, result in a HTTP `200`
    *OK* response with the error in the body of the message. Exceptions
    to this will be errors created by intermediate servers between the
    client and DATA API, such as Gateway errors.
3.  **Always JSON:** All messages passed between the client and server
    **must** be valid [JSON](https://www.json.org/) documents.

In general HTTP is treated as a synchronous message passing protocol,
rather than taking full advantage of it's features such as in a
[REST](https://en.wikipedia.org/wiki/Representational_state_transfer)
API. The motivations for this decision are:

1.  **Traffic Management:** By ensuring the message and the address to
    deliver it to are clearly separated we can more easily manage client
    traffic, both in real-time and off-line scenarios. Examples include
    gateways that multiplex traffic during online migration, shadow
    deployments for new server versions, capture-and-replay of
    production traffic for testing, or integration with Change Data
    Capture (CDC) systems.
2.  **API Portability:** By decoupling networking, encoding, and message
    delivery / RPC activation ("transport" defined in this HTTP API
    spec) from service functionality ("features" defined in the "JSON
    API Query Specification") alternative transports may more easily be
    added. A natural path for the DATA API may be to also provide a
    [gRPC](https://grpc.io/) as the [Stargate](https://stargate.io/)
    project already provides a gRPC transport for Cassandra's CQL. By
    not leveraging HTTP verbs or URL path and query patterns we provide
    a simpler public interface (i.e. a synchronous deliver message with
    reply) that can easily be replicated.

This approach is informed by both [gRPC](https://grpc.io/) and
[GraphQL](https://graphql.org/).

## High Level Concepts

Clients send a request message to the server over HTTP, and the server
sends a response message for each request received. The high level
concepts that are used to describe this process are:

-   **Endpoints:** The URL address for clients to send request messages.
    There are two types of endpoints which accept different messages:
    namespace and collection endpoints.
-   **Request Message:** A message from the client to the server
    requesting the server take some action, such as inserting or finding
    documents.
-   **Response Message:** A message from the server to the client
    responding to a specific client message, that includes status
    information such as affected documents and possibly the actual
    documents.

## Conventions

To aid in specifying the DATA API, we will use the following conventions
in this document:

-   Language rules will be given in a
    [BNF](http://en.wikipedia.org/wiki/Backus%E2%80%93Naur_Form)-like
    notation:

```bnf
<start> ::= TERMINAL <non-terminal1> <non-terminal1>
```

-   Nonterminal symbols will have `<angle brackets>`.
-   As additional shortcut notations to BNF, we'll use traditional
    regular expression's symbols (`?`, `+` and `*`) to signify that a
    given symbol is optional and/or can be repeated. We'll also allow
    parentheses to group symbols and the `[<characters>]` notation to
    represent any one of `<characters>`.
-   The grammar is provided for documentation purposes and leave some
    minor details out.
-   Sample code will be provided in a code block:

```cql
SELECT sample_usage FROM cql;
```

-   References to keywords or API examples text will be shown in a
    `fixed-width font`.

## Error Handling

The server will typically respond with a HTTP `200` *OK* status and encode
errors in the body of the response.

Client drivers should be aware of the situations where a request may
receive a non `200` response generated by servers between the client and
the Document Service. These errors should be in the `4XX` or `5XX` range and
include:

1.  `500` Internal Server Error - May be returned by intermediate cache
    or proxy servers.
2.  `501` Not Implemented
3.  `502` Bad Gateway
4.  `503` Service Unavailable
5.  `504` Gateway Timeout
6.  `401` Unauthorized 
7.  `404` Not Found 
8.  `405` Method Not Allowed 

All other errors, such as data validation or authorization errors, are
encoded in the response message. To avoid doubt these errors are called
"soft errors" in this document.

## Endpoints

The HTTP API provides two types of endpoints:

-   Namespace Endpoint: used to send commands which apply to the whole
    Namespace, such as listing all the Collections.
-   Collection Endpoint: used to send commands which apply to a single
    Collection, such as finding users by age.

Both endpoints follow these rules:

1.  All requests **must** use a HTTP `POST` verb.
2.  Requests **must** supply a `Token` header **TODO:** Why
    not just Authorization, current rest API is X-Cassandra-Token, using
    X is deprecated
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers
3.  Requests **must** be valid JSON documents and specify the
    `Content-Type:application/json; charset=UTF-8` header (see
    [RFC-8259](https://www.rfc-editor.org/rfc/rfc8259.html#page-11))
4.  Requests **must** specify the
    `Accept:application/json; charset=UTF-8` header

### Namespace Endpoint

The Namespace Endpoint has the form:

*Syntax:*

```bnf
<namespace-endpoint> ::= <server-address>/<namespace-name>
```

`<server-address>` is a Server URI using HTTP (S), scheme://host/path

`<namespace-name>` must begin with an alpha-numeric character and
can only contain alpha-numeric characters and underscores.

*Sample:*

`https://stargate.mycompany.com/my-namespace`

#### Namespace endpoint errors

Requests sent to a Namespace that was not previously created via administration tools results in a **TODO** soft error.

### Collection Endpoint

Collection Endpoint has the form:

*Syntax:*

```bnf
<collection-endpoint> ::= <server-address>/<namespace-name>/<collection-name>
```

`<collection-name>` must begin with an alpha-numeric character
and can only contain alpha-numeric characters and underscores.

*Sample:*

https://stargate.mycompany.com/my-namespace/users

Requests sent to a Collection that does not exist (using a valid
namespace name) result in the creation of the Collection and delivery of
the message. This may take longer than delivering a message to an
existing Collection.

**TODO** : Request and Response messages moved into the API Spec.

## Authentication and Authorization

**TODO** - basically we just use the tokens we have now.
