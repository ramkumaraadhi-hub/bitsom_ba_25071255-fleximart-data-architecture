// mongodb_operations.js (Part 2.2)

// Operation 1: Load Data
// Import products_catalog.json into 'products' collection
// Command (run in terminal):
// mongoimport --uri "<Your MongoDB URI>" --collection products --file products_catalog.json --jsonArray

// Operation 2: Basic Query
// Find all products in "Electronics" category with price < 50000
db.products.find(
  { category: "Electronics", price: { $lt: 50000 } },
  { name: 1, price: 1, stock: 1, _id: 0 }
);

// Operation 3: Review Analysis
// Find products with average rating >= 4.0
db.products.aggregate([
  { $unwind: "$reviews" },
  { $group: { _id: "$product_id", avgRating: { $avg: "$reviews.rating" }, name: { $first: "$name" } } },
  { $match: { avgRating: { $gte: 4.0 } } },
  { $project: { _id: 0, product_id: "$_id", name: 1, avgRating: 1 } }
]);

// Operation 4: Update Operation
// Add a new review to product "ELEC001"
db.products.updateOne(
  { product_id: "ELEC001" },
  { $push: { reviews: { user: "U999", rating: 4, comment: "Good value", date: new Date() } } }
);

// Operation 5: Complex Aggregation
// Calculate average price by category
db.products.aggregate([
  { $group: { _id: "$category", avg_price: { $avg: "$price" }, product_count: { $sum: 1 } } },
  { $project: { category: "$_id", avg_price: 1, product_count: 1, _id: 0 } },
  { $sort: { avg_price: -1 } }
]);
