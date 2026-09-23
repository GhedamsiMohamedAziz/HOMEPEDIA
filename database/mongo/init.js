// Runs once on first MongoDB container start (docker-entrypoint-initdb.d).
// Collections per section 8 of the plan; territory.commune_code is the canonical join key.
const dbName = process.env.MONGO_INITDB_DATABASE || "homepedia";
const db = db.getSiblingDB(dbName);

db.createCollection("raw_documents");
db.createCollection("reviews", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["source", "text"],
      properties: {
        source: { bsonType: "string" },
        text: { bsonType: "string" },
        language: { bsonType: "string" },
        territory: {
          bsonType: "object",
          properties: {
            commune_code: { bsonType: "string", pattern: "^[0-9AB]{5}$" },
            department_code: { bsonType: "string" },
            region_code: { bsonType: "string" },
          },
        },
        sentiment: { bsonType: ["double", "int"], minimum: -1, maximum: 1 },
        topics: { bsonType: "array", items: { bsonType: "string" } },
      },
    },
  },
});
db.createCollection("nlp_outputs");

db.raw_documents.createIndex({ source: 1, ingested_at: -1 });
db.reviews.createIndex({ "territory.commune_code": 1 });
db.reviews.createIndex({ source: 1 });
db.nlp_outputs.createIndex({ "territory.commune_code": 1, model: 1 });
