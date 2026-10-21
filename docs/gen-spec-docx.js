#!/usr/bin/env node
"use strict";
const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  HeadingLevel, AlignmentType, BorderStyle, WidthType, ShadingType,
  VerticalAlign, LevelFormat, PageNumber, Header, Footer,
  TableOfContents, ExternalHyperlink, PageBreak
} = require("docx");

// ── Colours ───────────────────────────────────────────────────────────────────
const BLUE_DARK  = "1F4E79";   // heading 1
const BLUE_MID   = "2E75B6";   // heading 2
const BLUE_LIGHT = "2F80B4";   // heading 3
const HEADER_BG  = "D6E4F0";
const CODE_BG    = "F4F6F8";
const WARN_BG    = "FFF8E1";
const TABLE_BORDER = { style: BorderStyle.SINGLE, size: 1, color: "C5D0DC" };

// ── Helpers ───────────────────────────────────────────────────────────────────
const cellBorders = { top: TABLE_BORDER, bottom: TABLE_BORDER, left: TABLE_BORDER, right: TABLE_BORDER };

function h1(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text, color: BLUE_DARK, bold: true })] });
}
function h2(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text, color: BLUE_MID, bold: true })] });
}
function h3(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text, color: BLUE_LIGHT, bold: true })] });
}
function h4(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_4, children: [new TextRun({ text, bold: true })] });
}
function para(text, opts = {}) {
  return new Paragraph({ spacing: { after: 120 }, children: [new TextRun({ text, ...opts })] });
}
function paraRuns(...runs) {
  return new Paragraph({ spacing: { after: 120 }, children: runs });
}
function boldPara(text) { return para(text, { bold: true }); }
function italicPara(text) { return para(text, { italics: true, color: "555555" }); }
function note(text) {
  return new Paragraph({
    spacing: { before: 120, after: 160 },
    indent: { left: 560, right: 560 },
    shading: { fill: WARN_BG, type: ShadingType.CLEAR },
    children: [new TextRun({ text: "ℹ  " + text, italics: true, size: 20, color: "5C4C00" })]
  });
}
function codeLine(text) {
  return new Paragraph({
    spacing: { after: 0 },
    indent: { left: 560 },
    shading: { fill: CODE_BG, type: ShadingType.CLEAR },
    children: [new TextRun({ text, font: "Courier New", size: 18, color: "1A1A1A" })]
  });
}
function codeBlock(lines) {
  return lines.map((l, i) => {
    const p = new Paragraph({
      spacing: { before: i === 0 ? 120 : 0, after: lines.length - 1 === i ? 160 : 0 },
      indent: { left: 560 },
      shading: { fill: CODE_BG, type: ShadingType.CLEAR },
      children: [new TextRun({ text: l, font: "Courier New", size: 18, color: "1A1A1A" })]
    });
    return p;
  });
}
function bullet(text, level = 0) {
  return new Paragraph({
    numbering: { reference: "bullets", level },
    spacing: { after: 60 },
    children: [new TextRun({ text })]
  });
}
function numbered(text, ref = "numbered") {
  return new Paragraph({
    numbering: { reference: ref, level: 0 },
    spacing: { after: 80 },
    children: [new TextRun({ text })]
  });
}
function divider() {
  return new Paragraph({ spacing: { before: 200, after: 200 }, border: { bottom: { style: BorderStyle.SINGLE, color: "C5D0DC", size: 1 } }, children: [new TextRun("")] });
}
function pageBreak() {
  return new Paragraph({ children: [new PageBreak()] });
}

// ── Table helpers ─────────────────────────────────────────────────────────────
function makeHeaderRow(labels, widths) {
  return new TableRow({
    tableHeader: true,
    children: labels.map((label, i) => new TableCell({
      borders: cellBorders,
      width: { size: widths[i], type: WidthType.DXA },
      shading: { fill: HEADER_BG, type: ShadingType.CLEAR },
      verticalAlign: VerticalAlign.CENTER,
      children: [new Paragraph({ alignment: AlignmentType.LEFT, spacing: { before: 80, after: 80 }, children: [new TextRun({ text: label, bold: true, size: 20 })] })]
    }))
  });
}
function makeDataRow(cells, widths) {
  return new TableRow({
    children: cells.map((text, i) => new TableCell({
      borders: cellBorders,
      width: { size: widths[i], type: WidthType.DXA },
      children: [new Paragraph({ spacing: { before: 60, after: 60 }, children: [new TextRun({ text: String(text), size: 20 })] })]
    }))
  });
}
function makeTable(headers, rows, widths) {
  return new Table({
    columnWidths: widths,
    margins: { top: 80, bottom: 80, left: 120, right: 120 },
    rows: [makeHeaderRow(headers, widths), ...rows.map(r => makeDataRow(r, widths))]
  });
}

// ── Content blocks ─────────────────────────────────────────────────────────────
function titlePage() {
  return [
    new Paragraph({ spacing: { before: 1440, after: 400 }, alignment: AlignmentType.CENTER, children: [new TextRun({ text: "Data API", bold: true, size: 72, color: BLUE_DARK, font: "Arial" })] }),
    new Paragraph({ spacing: { after: 200 }, alignment: AlignmentType.CENTER, children: [new TextRun({ text: "HCD OpenSearch Integration", bold: true, size: 48, color: BLUE_MID, font: "Arial" })] }),
    new Paragraph({ spacing: { after: 80 }, alignment: AlignmentType.CENTER, children: [new TextRun({ text: "Technical Specification", size: 32, color: "444444", font: "Arial" })] }),
    new Paragraph({ spacing: { after: 2880 }, alignment: AlignmentType.CENTER, children: [new TextRun({ text: new Date().toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" }), size: 24, color: "888888", font: "Arial" })] }),
    pageBreak()
  ];
}

function tocSection() {
  return [
    new TableOfContents("Table of Contents", { hyperlink: true, headingStyleRange: "1-4" }),
    pageBreak()
  ];
}

function section1() {
  return [
    h1("1. Top-Level Overview"),
    h2("Terminology — Two Names, Two Systems"),
    note("Every OpenSearch-backed table carries two completely separate index names that are used in different places and must never be confused."),
    makeTable(
      ["Term", "Definition", "Where it appears"],
      [
        ["SAI index name", "The Cassandra custom index identifier — the name after CREATE CUSTOM INDEX … IF NOT EXISTS. Used in CQL expr(<sai-index-name>, …) predicates. For collections always auto-generated as hcd_<keyspace>_<collectionName>. For tables it is the name field of createOpenSearchIndex.", "CQL statements, expr(...) calls, server-side routing"],
        ["OS index name", "The OpenSearch index name — the value of 'indexName' in WITH OPTIONS. This is the index that OpenSearch actually stores and searches. Defaults to hcd_<keyspace>_<tableName> when omitted.", "OpenSearch REST API, WITH OPTIONS 'indexName'"]
      ],
      [2400, 4200, 2760]
    ),
    para(""),
    para("Goal: Extend the Data API to expose the new HCD OpenSearch replication capability, which mirrors Cassandra writes to an OpenSearch index in real time. This surfaces ten impacted API surfaces across collections and tables."),
    para(""),
    h3("Collections (§4)"),
    bullet("createCollection — new openSearch option block configures CREATE CUSTOM INDEX … USING 'OpenSearchIndex' on the collection table."),
    bullet("findCollections — with explain: true, returns the resolved openSearch block for OpenSearch-enabled collections."),
    bullet("alterCollection — new operations addOpenSearch / dropOpenSearch to add or remove an OpenSearch index post-creation."),
    bullet("Collection find — new $openSearch operator in the filter clause routing queries to OpenSearch via CQL expr()."),
    bullet("searchAggregate — new command passing OpenSearch aggregation DSL through expr() and returning facet/metric results."),
    bullet("countDocuments with $openSearch — returns the OpenSearch numFound count."),
    bullet("deleteCollection — also drops the OpenSearch custom index before dropping the table."),
    para(""),
    h3("Tables (§5)"),
    bullet("createOpenSearchIndex — dedicated DDL command (parallel to createTextIndex / createVectorIndex)."),
    bullet("Table find — $openSearch operator on table reads, backed by expr()."),
    bullet("dropIndex — must handle indexType: openSearch."),
    para(""),
    note("$openSearch is exclusive — it cannot be combined with regular field filters. The $openSearch key must always be the only key in the filter object. This is a fundamental CQL constraint: HCD's expr(...) predicate takes full ownership of the read path."),
    para(""),
    h3("Scope Constraints (Not Covered)"),
    bullet("Any changes to the HCD / OpenSearch replication component itself."),
    bullet("Dynamic schema / custom mapping management."),
    bullet("Backfill / rebuild operations."),
    bullet("OpenSearch highlighting, autocomplete/suggest, and scroll cursor."),
  ];
}

function section2() {
  return [
    pageBreak(),
    h1("2. Architecture Overview"),
    h2("Write Path"),
    note("Every document write is automatically mirrored to OpenSearch by HCD with no Data API involvement beyond the initial DDL."),
    para("Flow: Client → Data API (normal CQL write) → Cassandra → HCD OpenSearchMutatorInterceptor → BulkProcessor (batches up to 1,000 docs, flushes every 1s) → OpenSearch Bulk Index API."),
    para(""),
    h2("Read Path"),
    note("$openSearch, searchAggregate, and countDocuments with $openSearch all follow this path."),
    para("Flow: Client → Data API (builds expr(saiIndexName, dsl)) → Cassandra → HCD OpenSearchQueryHandler (intercepts expr() predicate) → OpenSearch (/_search or /_count) → matching doc PKs → Cassandra rows → Data API response."),
    para(""),
    h2("Key Constraint"),
    note("expr(...) must be the sole predicate — no AND with regular column filters. Because the handler owns the full result set, expr(...) cannot be mixed with regular CQL predicates in the same WHERE clause."),
  ];
}

function section3() {
  return [
    pageBreak(),
    h1("3. Context — How HCD-to-OpenSearch Works"),
    para("The HCD OpenSearch component hooks into the Cassandra write path (OpenSearchMutatorInterceptor) and replicates every mutation in real time to a connected OpenSearch cluster. The feature is activated per-table by creating a single special Cassandra custom index."),
    para(""),
    boldPara("CQL index creation pattern:"),
    ...codeBlock([
      "CREATE CUSTOM INDEX IF NOT EXISTS <sai-index-name>",
      "ON <keyspace>.<table> (<columns>)",
      "USING 'OpenSearchIndex'",
      "WITH OPTIONS = {",
      "    'indexName':              '<os-index-name>',",
      "    'createIndexIfNotExists': 'true',",
      "    'unpackJsonFields':       'doc_json',",
      "    'propertiesFromJsonFields': 'field1,field2'",
      "};"
    ]),
    para(""),
    boldPara("Query pattern:"),
    ...codeBlock(["SELECT * FROM <table> WHERE expr(<sai-index-name>, '<OpenSearch-query-JSON>')"]),
    para(""),
    h2("3.1 Constraints Specific to Data API Collections"),
    para("The collection shredding model stores the raw JSON in doc_json TEXT alongside auxiliary indexing columns. The following four WITH OPTIONS keys are always required for collections:"),
    para(""),
    numbered("unpackJsonFields: 'doc_json' — HCD must parse the JSON blob and merge properties at the OS document root.", "col-numbered"),
    numbered("propertiesFromJsonFields: '<fields>' — allow-list of fields to extract. Must never include _id.", "col-numbered"),
    numbered("applyCustomSchema: 'true' + customMappingsJson: '<json>' — required because the Cassandra column is doc_json TEXT with no typed schema.", "col-numbered"),
    numbered("applyDefaultSchema: 'false' — must accompany applyCustomSchema: 'true'.", "col-numbered"),
    para(""),
    note("_id handling: Every Data API document carries an _id field in doc_json. OpenSearch always rejects _id as a body field because it is a reserved metadata field. The workaround is propertiesFromJsonFields, which acts as an allow-list that excludes _id."),
  ];
}

function section4_1() {
  return [
    pageBreak(),
    h1("4. Impacted Commands — Collections"),
    h2("4.1 createCollection — New openSearch Option Block"),
    h3("Goal"),
    para("createCollection gains a new optional openSearch block inside options. Setting enabled: true tells the Data API to create an OpenSearch replication index alongside the normal Cassandra collection table. From that point on, every document write is automatically mirrored to OpenSearch by HCD."),
    para(""),
    h3("API Fields"),
    makeTable(
      ["Field", "Required", "Default", "Description"],
      [
        ["enabled", "yes", "—", "true activates OpenSearch replication."],
        ["indexName", "no", "hcd_<keyspace>_<collectionName>", "OS index name — the OpenSearch index name. The SAI index name is always auto-generated and cannot be configured."],
        ["numShards", "no", "HCD default", "Number of primary shards for the OS index."],
        ["numReplicas", "no", "HCD default", "Number of replicas for the OS index."],
        ["mappings", "yes", "—", "Map of top-level document field name → OpenSearch field definition. _id must never appear as a key."]
      ],
      [1600, 900, 2000, 4860]
    ),
    para(""),
    h3("OpenSearch Field Types"),
    makeTable(
      ["Type", "Use for", "Example"],
      [
        ["text", "Full-text search with analysis", '{"type":"text"} or {"type":"text","analyzer":"english"}'],
        ["keyword", "Exact match, sorting, aggregations", '{"type":"keyword"}'],
        ["integer / long", "Whole numbers (32/64-bit)", '{"type":"integer"}'],
        ["float / double", "Decimal numbers (32/64-bit)", '{"type":"float"}'],
        ["boolean", "true / false", '{"type":"boolean"}'],
        ["date", "ISO-8601 strings or epoch millis", '{"type":"date"}'],
        ["object", "Nested JSON object — sub-fields queryable as parent.child", '{"type":"object"} or {"properties":{...}}'],
        ["nested", "Array of objects (avoids cross-element matches)", '{"type":"nested"}'],
        ["knn_vector", "Dense vector for k-NN", '{"type":"knn_vector","dimension":1536}']
      ],
      [1400, 2800, 5160]
    ),
    para(""),
    h3("Sample Payload — Minimal"),
    ...codeBlock([
      '{',
      '  "createCollection": {',
      '    "name": "persons",',
      '    "options": {',
      '      "openSearch": {',
      '        "enabled": true,',
      '        "mappings": {',
      '          "firstName": { "type": "text" },',
      '          "lastName":  { "type": "text" },',
      '          "city":      { "type": "text" },',
      '          "age":       { "type": "integer" }',
      '        }',
      '      }',
      '    }',
      '  }',
      '}'
    ]),
    para(""),
    h3("CQL Emitted"),
    ...codeBlock([
      "CREATE CUSTOM INDEX IF NOT EXISTS hcd_mykeyspace_persons",
      "ON mykeyspace.persons (doc_json)",
      "USING 'OpenSearchIndex'",
      "WITH OPTIONS = {",
      "  'indexName':               'hcd_mykeyspace_persons',",
      "  'createIndexIfNotExists':  'true',",
      "  'unpackJsonFields':        'doc_json',",
      "  'applyDefaultSchema':      'false',",
      "  'applyCustomSchema':       'true',",
      "  'customMappingsJson':      '{\"properties\":{\"_id\":{\"type\":\"keyword\"},\"firstName\":{\"type\":\"text\"},...}}',",
      "  'propertiesFromJsonFields':'firstName,lastName,city,age'",
      "};"
    ]),
    para(""),
    h3("Key Constraints"),
    bullet("mappings is always required — there is no auto-inference path for collections."),
    bullet("_id must never appear as a key in mappings — it is reserved by OpenSearch."),
    bullet("indexing.deny is rejected when openSearch.enabled: true (use indexing.allow instead)."),
    bullet("When indexing.allow is present, propertiesFromJsonFields is filtered to the intersection of mappings keys and indexing.allow."),
  ];
}

function section4_2() {
  return [
    pageBreak(),
    h2("4.2 findCollections — Return openSearch Block in explain Response"),
    h3("Goal"),
    para("When a collection was created with openSearch.enabled: true, the openSearch block is returned as part of its definition when findCollections is called with options.explain: true. The openSearch key is absent for collections that have no OpenSearch index."),
    para(""),
    h3("Response Fields"),
    makeTable(
      ["Field", "Always Present", "Notes"],
      [
        ["enabled", "yes", "Always true when the block is present."],
        ["indexName", "yes", "Resolved OS index name — never null."],
        ["numShards", "yes", "Persisted at creation; always returned."],
        ["numReplicas", "yes", "Persisted at creation; always returned."],
        ["mappings", "yes", "Always persisted and always returned."]
      ],
      [1800, 1600, 6000]
    ),
    para(""),
    h3("Sample Response"),
    ...codeBlock([
      '{',
      '  "status": {',
      '    "collections": [',
      '      {',
      '        "name": "articles",',
      '        "options": {',
      '          "openSearch": {',
      '            "enabled": true,',
      '            "numShards": 1,',
      '            "numReplicas": 1,',
      '            "indexName": "articles-full-text",',
      '            "mappings": {',
      '              "title":  { "type": "text", "analyzer": "english" },',
      '              "body":   { "type": "text", "analyzer": "english" },',
      '              "author": { "type": "keyword" }',
      '            }',
      '          }',
      '        }',
      '      }',
      '    ]',
      '  }',
      '}'
    ]),
  ];
}

function section4_3() {
  return [
    pageBreak(),
    h2("4.3 alterCollection — Add or Drop OpenSearch Index"),
    h3("Goal"),
    para("alterCollection is extended with two new operations — addOpenSearch and dropOpenSearch — that allow callers to attach or remove an OpenSearch replication index on a collection that already exists, without recreating the collection."),
    note("OpenSearch replication is mutable after collection creation. Unlike lexical or rerank (which are fixed at creation), the openSearch block is explicitly designed to be added and removed post-creation via alterCollection."),
    para(""),
    h3("addOpenSearch Fields"),
    makeTable(
      ["Field", "Required", "Default", "Description"],
      [
        ["operation.addOpenSearch.enabled", "yes", "—", "Must be true."],
        ["operation.addOpenSearch.indexName", "no", "hcd_<keyspace>_<collectionName>", "OS index name."],
        ["operation.addOpenSearch.numShards", "no", "HCD default", "Number of primary shards."],
        ["operation.addOpenSearch.numReplicas", "no", "HCD default", "Number of replicas."],
        ["operation.addOpenSearch.mappings", "yes", "—", "Required. Must be non-empty and must not contain _id."]
      ],
      [2800, 900, 2000, 3660]
    ),
    para(""),
    h3("Sample Payloads"),
    boldPara("Add OpenSearch:"),
    ...codeBlock([
      '{ "alterCollection": { "operation": {',
      '    "addOpenSearch": {',
      '      "enabled": true,',
      '      "indexName": "articles-full-text",',
      '      "mappings": {',
      '        "title":  { "type": "text", "analyzer": "english" },',
      '        "author": { "type": "keyword" }',
      '      }',
      '    }',
      '} } }'
    ]),
    para(""),
    boldPara("Drop OpenSearch:"),
    ...codeBlock(['{ "alterCollection": { "operation": { "dropOpenSearch": {} } } }']),
    para(""),
    h3("Error Cases"),
    makeTable(
      ["Situation", "Error Code"],
      [
        ["addOpenSearch on a collection that already has OpenSearch enabled", "OPEN_SEARCH_INDEX_ALREADY_EXISTS"],
        ["dropOpenSearch on a collection with no OpenSearch index", "OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION"]
      ],
      [5400, 4000]
    ),
    para(""),
    h3("Idempotency and createCollection Convergence"),
    boldPara("Scenario A — createCollection after alterCollection addOpenSearch:"),
    para("If the collection already has OpenSearch enabled and createCollection is called with the same openSearch block → idempotent success. If the block differs → EXISTING_COLLECTION_DIFFERENT_SETTINGS."),
    boldPara("Scenario B — alterCollection addOpenSearch after createCollection with openSearch:"),
    para("If the collection already has OpenSearch enabled → OPEN_SEARCH_INDEX_ALREADY_EXISTS. The client should use dropOpenSearch first if reconfiguration is needed."),
  ];
}

function section4_4_to_4_7() {
  return [
    pageBreak(),
    h2("4.4 find on Collections — New $openSearch Filter Operator"),
    h3("Goal"),
    para("A new $openSearch operator in the filter clause routes the query to OpenSearch via CQL expr(<saiIndexName>, '<OpenSearch JSON>'). The $openSearch key must always be the only key in the filter object."),
    note("$openSearch cannot be combined with any other filter key. This is a fundamental CQL/HCD constraint."),
    para(""),
    h3("Filter Operators"),
    makeTable(
      ["Operator", "OpenSearch mapping", "Description"],
      [
        ["$openSearch.$match", "match query", "Full-text match on a single field"],
        ["$openSearch.$matchAll", "multi_match (best_fields)", "Match across multiple fields"],
        ["$openSearch.$matchPhrase", "match_phrase", "Exact phrase search"],
        ["$openSearch.$term", "term", "Exact (keyword) match"],
        ["$openSearch.$terms", "terms", "Match any of a list of exact values"],
        ["$openSearch.$range", "range", "Numeric or date range"],
        ["$openSearch.$exists", "exists", "Field has any non-null value"],
        ["$openSearch.$bool", "bool", "Combine queries with must/should/must_not/filter"],
        ["$openSearch.$nested", "nested", "Query inside nested objects"],
        ["$openSearch.$sort", "sort clause", "Order results by score or field value"],
        ["$openSearch.$limit", "size", "Max number of results (default 20, max 1000)"],
        ["$openSearch.$skip", "from", "Offset for pagination"],
        ["$openSearch.$searchAfter", "search_after", "Keyset cursor pagination"]
      ],
      [2200, 2000, 5160]
    ),
    para(""),
    h3("Sample Payload — Match Query"),
    ...codeBlock([
      '{ "find": { "filter": { "$openSearch": { "$match": { "title": "OpenSearch" } } } } }'
    ]),
    para(""),
    h3("Paging"),
    note("Because expr(...) returns an unbounded result set to Cassandra, the Data API omits the CQL LIMIT entirely for $openSearch queries. Use $openSearch.$limit (max 1,000) to control result count."),
    bullet("$skip / $limit: offset-based pagination (suitable for up to 10,000 documents)."),
    bullet("$searchAfter: keyset cursor pagination using sort fields — preferred for deep pagination."),
    para(""),
    divider(),
    h2("4.5 searchAggregate — Aggregations and Faceted Search"),
    h3("Goal"),
    para("A new command that passes OpenSearch aggregation DSL (aggs) through expr(...) and returns structured facet / metric results. Requires OpenSearch to be enabled on the collection."),
    para(""),
    h3("Aggregation Types"),
    makeTable(
      ["Agg type", "OpenSearch mapping", "Description"],
      [
        ["terms", "terms aggregation", "Faceted counts by distinct value (top-N buckets)"],
        ["date_histogram", "date_histogram", "Group documents by time period"],
        ["histogram", "histogram", "Group numeric values into fixed-width buckets"],
        ["range", "range aggregation", "Group values into explicit range buckets"],
        ["min / max / avg / sum", "metric aggregations", "Single-value statistics"],
        ["extended_stats", "extended_stats", "Mean, stddev, min, max, variance, sum"],
        ["top_hits", "top_hits", "Retrieve sample documents from each bucket"],
        ["nested", "nested", "Aggregate inside nested objects"]
      ],
      [2200, 2200, 5000]
    ),
    para(""),
    h3("Sample Payload"),
    ...codeBlock([
      '{',
      '  "searchAggregate": {',
      '    "aggregations": {',
      '      "authors": { "terms": { "field": "author", "size": 10 } },',
      '      "avg_score": { "avg": { "field": "score" } }',
      '    },',
      '    "filter": { "$openSearch": { "$match": { "title": "distributed systems" } } }',
      '  }',
      '}'
    ]),
    para(""),
    divider(),
    h2("4.6 countDocuments with $openSearch Filter"),
    h3("Goal"),
    para("The existing countDocuments command is extended to accept a $openSearch filter and return the OpenSearch numFound count via SELECT COUNT(*) … WHERE expr(<saiIndexName>, ?)."),
    para(""),
    h3("Sample Payload"),
    ...codeBlock([
      '{',
      '  "countDocuments": {',
      '    "filter": { "$openSearch": { "$match": { "title": "distributed systems" } } }',
      '  }',
      '}'
    ]),
    para(""),
    divider(),
    h2("4.7 deleteCollection — Also Drops the OpenSearch Index"),
    h3("Behaviour"),
    para("When deleting a collection that has OpenSearch enabled, the Data API first drops the OpenSearch custom index (DROP INDEX IF EXISTS hcd_<ks>_<coll>) before dropping the table. This causes HCD to also delete the OS index from the OpenSearch cluster."),
    note("The API signature is unchanged — the caller does not need to specify anything extra. The additional DROP INDEX step is transparent."),
  ];
}

function section5() {
  return [
    pageBreak(),
    h1("5. Impacted Commands — Tables"),
    h2("5.1 createOpenSearchIndex — New DDL Command for Tables"),
    h3("Goal"),
    para("A new dedicated createOpenSearchIndex command is added for tables, following the same pattern as createTextIndex and createVectorIndex. Only one OpenSearch custom index per table is permitted by HCD."),
    note("For tables: the SAI index name is user-configurable (the name field). The OS index name is definition.options.openSearchIndexName, defaulting to hcd_<keyspace>_<tableName>."),
    para(""),
    h3("API Fields"),
    makeTable(
      ["Field", "Required", "Default", "Description"],
      [
        ["name", "yes", "—", "SAI index name — the Cassandra custom index identifier used in expr() predicates."],
        ["definition.columns", "no", "all non-PK columns", "List of table columns to replicate."],
        ["definition.options.openSearchIndexName", "no", "hcd_<keyspace>_<tableName>", "OS index name (OpenSearch side). Renamed from indexName to avoid ambiguity."],
        ["definition.options.numShards", "no", "HCD default", "Number of primary shards."],
        ["definition.options.numReplicas", "no", "HCD default", "Number of replicas."],
        ["definition.options.mappings", "no", "HCD infers from schema", "Optional field mapping object. HCD uses applyDefaultSchema: true when absent."],
        ["definition.options.applyDefaultSchema", "no", "true", "Set false to rely on an existing OS index template."],
        ["options.ifNotExists", "no", "false", "Do not error if the SAI index name already exists."]
      ],
      [2800, 900, 2100, 3560]
    ),
    para(""),
    h3("WITH OPTIONS Rules"),
    makeTable(
      ["Condition", "Option key added"],
      [
        ["Always", "createIndexIfNotExists: 'true'"],
        ["definition.options.openSearchIndexName set", "indexName: '<value>'"],
        ["definition.options.numShards set", "numShards: '<value>'"],
        ["definition.options.numReplicas set", "numReplicas: '<value>'"],
        ["definition.options.mappings set", "applyCustomSchema: 'true', applyDefaultSchema: 'false', customMappingsJson: '<wrapped-json>'"],
        ["definition.options.applyDefaultSchema: false", "applyDefaultSchema: 'false'"],
        ["collections only", "unpackJsonFields is NOT set for tables — tables have typed columns"]
      ],
      [3600, 5760]
    ),
    para(""),
    h3("Sample Payloads"),
    boldPara("Minimal — index all non-PK columns:"),
    ...codeBlock(['{ "createOpenSearchIndex": { "name": "users_search" } }']),
    para(""),
    boldPara("Specific columns with custom mappings:"),
    ...codeBlock([
      '{',
      '  "createOpenSearchIndex": {',
      '    "name": "articles_search",',
      '    "definition": {',
      '      "columns": ["title", "body", "author"],',
      '      "options": {',
      '        "openSearchIndexName": "articles-full-text",',
      '        "mappings": {',
      '          "title":  { "type": "text", "analyzer": "english" },',
      '          "body":   { "type": "text", "analyzer": "english" },',
      '          "author": { "type": "keyword" }',
      '        }',
      '      }',
      '    },',
      '    "options": { "ifNotExists": true }',
      '  }',
      '}'
    ]),
    para(""),
    boldPara("listIndexes response showing an OpenSearch index:"),
    ...codeBlock([
      '{ "status": { "indexes": [',
      '  { "name": "articles_title_sai", "indexType": "regular", "definition": { "column": "title" } },',
      '  { "name": "articles_search",    "indexType": "openSearch",',
      '    "definition": { "columns": ["title","body","author"],',
      '      "options": { "openSearchIndexName": "articles-full-text" } } }',
      '] } }'
    ]),
    para(""),
    h2("5.2 find on Tables — $openSearch Filter Operator"),
    h3("Goal"),
    para("The same $openSearch operator available on collections is also available on table reads, backed by expr(<saiIndexName>, ...) where saiIndexName is the user-supplied name from createOpenSearchIndex."),
    note("For tables, the $openSearch filter must reference the SAI index name chosen at createOpenSearchIndex time. The resolver looks up the OpenSearch index on the table schema and injects the correct saiIndexName into the CQL expr() predicate automatically."),
    para(""),
    h3("Sample Payload"),
    ...codeBlock([
      '{',
      '  "find": {',
      '    "filter": {',
      '      "$openSearch": {',
      '        "$match": { "title": "distributed systems" }',
      '      }',
      '    }',
      '  }',
      '}'
    ]),
    para(""),
    h2("5.3 dropIndex on Tables — Drops the OpenSearch Custom Index"),
    para("The existing dropIndex command handles indexType: openSearch without any API surface change. Dropping the Cassandra custom index causes HCD to also delete the OS index from the OpenSearch cluster."),
    ...codeBlock([
      '{ "dropIndex": { "name": "articles_search" } }'
    ]),
  ];
}

function section6() {
  return [
    pageBreak(),
    h1("6. Cross-Cutting Concerns"),
    h2("6.1 Class Hierarchy"),
    boldPara("Collections:"),
    bullet("CollectionOpenSearchDef — runtime schema object (persisted in table comment JSON)"),
    bullet("CollectionOpenSearchDefSchemaFactory — factory following CollectionLexicalDefSchemaFactory pattern"),
    bullet("CollectionSettingsV3Reader — reads V_3 schema version including openSearch block"),
    bullet("CollectionSchemaVersion.V_3 — new schema version for OpenSearch"),
    para(""),
    boldPara("Tables:"),
    bullet("ApiOpenSearchIndex implements ApiIndexDef — multi-column index (not single-column like ApiSupportedIndex)"),
    bullet("ApiIndexType.OPEN_SEARCH — new enum value"),
    bullet("OpenSearchIndexDefinitionDesc — command-level descriptor for createOpenSearchIndex"),
    bullet("CreateOpenSearchIndexCommandResolver — validates and delegates to CreateIndexDBTask"),
    para(""),
    boldPara("Filters:"),
    bullet("OpenSearchCollectionFilter — collection $openSearch filter"),
    bullet("OpenSearchTableFilter — table $openSearch filter"),
    bullet("Both bypass normal column/PK/index validation (WhereCQLClauseAnalyzer short-circuit)"),
    para(""),
    h2("6.2 Schema Version Evolution"),
    makeTable(
      ["Version", "Introduced", "Changes"],
      [
        ["V_minus", "Legacy", "No table comment at all"],
        ["V_0", "Legacy", "JSON comment without 'collection' top-level key"],
        ["V_1", "First versioned", "Basic collection schema with name, options, schema_version"],
        ["V_2", "Lexical/rerank", "Added lexical and rerank blocks"],
        ["V_3", "OpenSearch", "Added openSearch block; CURRENT_VERSION"]
      ],
      [1400, 1800, 6160]
    ),
    para(""),
    h2("6.3 Error Codes"),
    makeTable(
      ["Error Code", "Trigger"],
      [
        ["OPEN_SEARCH_INDEX_ALREADY_EXISTS", "addOpenSearch on a collection/table that already has one; createOpenSearchIndex duplicate"],
        ["OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION", "dropOpenSearch or $openSearch on a collection without OpenSearch"],
        ["OPEN_SEARCH_MISSING_FIELD_MAPPINGS", "mappings absent/empty/contains _id; empty intersection with indexing.allow"],
        ["OPEN_SEARCH_INCOMPATIBLE_WITH_DENY_LIST", "openSearch.enabled: true combined with indexing.deny"],
        ["OPEN_SEARCH_CORRUPT_SCHEMA", "Stored comment missing required fields (indexName, customMappingsJson)"],
        ["EXISTING_COLLECTION_DIFFERENT_SETTINGS", "createCollection on existing collection with different openSearch block"]
      ],
      [3400, 6000]
    ),
    para(""),
    h2("6.4 Write Consistency"),
    note("HCD automatically upgrades write consistency for OS-indexed tables: ANY / ONE / LOCAL_ONE → QUORUM. This is transparent to the Data API and client applications."),
    para(""),
    boldPara("Eventual consistency — not read-my-writes:"),
    para("The OpenSearch index is updated asynchronously via HCD's BulkProcessor, which batches up to 1,000 documents and flushes every 1 second. A document inserted and immediately searched via $openSearch may not yet appear in results. The default refresh interval on HCD-managed indices is 1 second."),
    para(""),
    h2("6.5 Testing"),
    boldPara("Unit test coverage required:"),
    bullet("createCollection with openSearch block — CQL emitted correctly"),
    bullet("findCollections explain: true — openSearch block round-tripped"),
    bullet("alterCollection addOpenSearch — CQL emitted, comment updated"),
    bullet("alterCollection dropOpenSearch — DROP INDEX emitted, comment updated"),
    bullet("$openSearch filter — expr() predicate built correctly"),
    bullet("searchAggregate — aggregation DSL passed through"),
    bullet("countDocuments with $openSearch — numFound returned"),
    bullet("deleteCollection — DROP INDEX before DROP TABLE"),
    bullet("createOpenSearchIndex — table variant, CQL emitted correctly"),
    bullet("Error cases: missing mappings, _id in mappings, duplicate index, deny-list conflict"),
  ];
}

function section7() {
  return [
    pageBreak(),
    h1("7. Appendix — OpenSearchQueryBuilder API Reference"),
    h2("7.1 Query Builders"),
    makeTable(
      ["Builder", "OpenSearch equivalent", "Key parameters"],
      [
        ["$match", "match", "field, query, operator (and/or), fuzziness"],
        ["$matchAll", "multi_match (best_fields)", "fields[], query, type"],
        ["$matchPhrase", "match_phrase", "field, query, slop"],
        ["$term", "term", "field, value"],
        ["$terms", "terms", "field, values[]"],
        ["$range", "range", "field, gte, lte, gt, lt, format"],
        ["$exists", "exists", "field"],
        ["$bool", "bool", "must[], should[], must_not[], filter[], minimum_should_match"],
        ["$nested", "nested", "path, query, score_mode"]
      ],
      [1800, 2400, 5160]
    ),
    para(""),
    h2("7.2 Aggregation Builders"),
    makeTable(
      ["Builder", "OpenSearch equivalent", "Key parameters"],
      [
        ["terms", "terms", "field, size, order"],
        ["date_histogram", "date_histogram", "field, calendar_interval / fixed_interval, format"],
        ["histogram", "histogram", "field, interval"],
        ["range", "range", "field, ranges[{from, to, key}]"],
        ["min / max / avg / sum", "metric aggregations", "field"],
        ["extended_stats", "extended_stats", "field, sigma"],
        ["top_hits", "top_hits", "_source, size, sort"],
        ["nested", "nested", "path"]
      ],
      [1800, 2400, 5160]
    ),
    para(""),
    h2("7.3 Sort Builders"),
    makeTable(
      ["Builder", "OpenSearch equivalent", "Key parameters"],
      [
        ["$sort.$score", "_score", "order: desc (default)"],
        ["$sort.$field", "field sort", "field, order (asc/desc), missing (_last/_first)"],
        ["$sort.$geo_distance", "_geo_distance", "field, location, order, unit"],
        ["$sort.$nested", "nested sort", "path, filter, nested"],
        ["$sort.$script", "script sort", "script, type (number/string), order"]
      ],
      [1800, 2400, 5160]
    ),
    para(""),
    h2("7.4 Cross-Language Usage Summary"),
    makeTable(
      ["Language", "Import", "Usage"],
      [
        ["Java", "astrapy.tables.OpenSearchQueryBuilder", 'filter("$openSearch", OpenSearchQueryBuilder.match("title", "foo"))'],
        ["Python", "astrapy.tables.OpenSearchQueryBuilder", 'filter={"$openSearch": {"$match": {"title": "foo"}}}'],
        ["TypeScript", "@datastax/astra-db-ts", 'filter: { $openSearch: { $match: { title: "foo" } } }']
      ],
      [1400, 3200, 4760]
    ),
  ];
}

// ── Assemble document ─────────────────────────────────────────────────────────
const children = [
  ...titlePage(),
  ...tocSection(),
  ...section1(),
  ...section2(),
  ...section3(),
  ...section4_1(),
  ...section4_2(),
  ...section4_3(),
  ...section4_4_to_4_7(),
  ...section5(),
  ...section6(),
  ...section7(),
];

const doc = new Document({
  styles: {
    default: {
      document: { run: { font: "Arial", size: 22 } }
    },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 36, bold: true, font: "Arial", color: BLUE_DARK },
        paragraph: { spacing: { before: 360, after: 180 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 28, bold: true, font: "Arial", color: BLUE_MID },
        paragraph: { spacing: { before: 280, after: 140 }, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 24, bold: true, font: "Arial", color: BLUE_LIGHT },
        paragraph: { spacing: { before: 200, after: 100 }, outlineLevel: 2 } },
      { id: "Heading4", name: "Heading 4", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 22, bold: true, font: "Arial" },
        paragraph: { spacing: { before: 160, after: 80 }, outlineLevel: 3 } },
    ]
  },
  numbering: {
    config: [
      { reference: "bullets",
        levels: [
          { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 560, hanging: 280 } } } },
          { level: 1, format: LevelFormat.BULLET, text: "◦", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 1120, hanging: 280 } } } }
        ]
      },
      { reference: "numbered",
        levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 560, hanging: 280 } } } }]
      },
      { reference: "col-numbered",
        levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 560, hanging: 280 } } } }]
      }
    ]
  },
  sections: [{
    properties: {
      page: { margin: { top: 1080, right: 1080, bottom: 1080, left: 1080 } }
    },
    headers: {
      default: new Header({ children: [
        new Paragraph({ alignment: AlignmentType.RIGHT, spacing: { after: 0 },
          border: { bottom: { style: BorderStyle.SINGLE, color: "C5D0DC", size: 1 } },
          children: [new TextRun({ text: "Data API — HCD OpenSearch Integration", size: 18, color: "666666" })] })
      ]})
    },
    footers: {
      default: new Footer({ children: [
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 0 },
          border: { top: { style: BorderStyle.SINGLE, color: "C5D0DC", size: 1 } },
          children: [
            new TextRun({ text: "Page ", size: 18, color: "666666" }),
            new TextRun({ children: [PageNumber.CURRENT], size: 18, color: "666666" }),
            new TextRun({ text: " of ", size: 18, color: "666666" }),
            new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 18, color: "666666" })
          ]
        })
      ]})
    },
    children
  }]
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync("docs/hcd-opensearch-data-api-spec.docx", buf);
  console.log("✅  Written: docs/hcd-opensearch-data-api-spec.docx");
});
