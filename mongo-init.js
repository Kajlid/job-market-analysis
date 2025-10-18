// Create root/admin user if not exists
db = db.getSiblingDB('admin');
if (db.getUser(process.env.MONGO_INITDB_ROOT_USERNAME || "root") === null) {
  db.createUser({
    user: process.env.MONGO_INITDB_ROOT_USERNAME || "root",
    pwd: process.env.MONGO_INITDB_ROOT_PASSWORD || "password",
    roles: [{ role: "root", db: "admin" }]
  });
  print("✅ Root user created");
} else {
  print("ℹ️ Root user already exists — skipping creation");
}

// Switch to your app database
db = db.getSiblingDB(process.env.MONGO_INITDB_DATABASE || "jobmarket");
db.createCollection("global_clusters");

// Create app user if not exists
if (db.getUser(process.env.MONGO_INITDB_ROOT_USERNAME || "root") === null) {
  db.createUser({
    user: process.env.MONGO_INITDB_ROOT_USERNAME || "root",
    pwd: process.env.MONGO_INITDB_ROOT_PASSWORD || "password",
    roles: [{ role: "readWrite", db: process.env.MONGO_INITDB_DATABASE || "jobmarket" }]
  });
  print("✅ App user created");
} else {
  print("ℹ️ App user already exists — skipping creation");
}
