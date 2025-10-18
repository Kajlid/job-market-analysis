db = db.getSiblingDB('admin');
// Create root user if it doesn't exist
db.createUser({
  user: process.env.MONGO_INITDB_ROOT_USERNAME || "root",
  pwd: process.env.MONGO_INITDB_ROOT_PASSWORD || "password",
  roles: [{ role: "root", db: "admin" }]
});

// db.auth(process.env.MONGO_INITDB_ROOT_USERNAME, process.env.MONGO_INITDB_ROOT_PASSWORD);
// Switch to your app database
db = db.getSiblingDB(process.env.MONGO_INITDB_DATABASE || "jobmarket");
db.createCollection("global_clusters");

// db = db.getSiblingDB(process.env.MONGO_INITDB_DATABASE);
// db.createCollection('global_clusters');

// Create user for the specific database
// Create app user
db.createUser({
  user: process.env.MONGO_INITDB_ROOT_USERNAME || "root",
  pwd: process.env.MONGO_INITDB_ROOT_PASSWORD || "password",
  roles: [{ role: "readWrite", db: process.env.MONGO_INITDB_DATABASE || "mydb" }]
});