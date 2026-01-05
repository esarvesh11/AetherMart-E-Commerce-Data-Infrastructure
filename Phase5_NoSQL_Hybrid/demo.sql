// --- 1. CREATE ---
// Insert the new "Quantum Echo Speaker" product
db.products.insertOne({
  product_name: "Quantum Echo Speaker",
  description: "AI-powered smart home hub and speaker.",
  price: 179.99,
  category_id: 2,
  stock_level: 350,
  created_at: new Date(),
  tags: ["smart_home", "audio", "AI", "top_seller"]
})


// --- 2. READ ---
// Find the product to verify it was created
db.products.findOne({ product_name: "Quantum Echo Speaker" })


// --- 3. UPDATE ---
// Apply a price drop for a sale and decrement stock after 20 units are sold
db.products.updateOne(
  { product_name: "Quantum Echo Speaker" },
  {
    $set: { price: 169.99 },    // Set a new sale price
    $inc: { stock_level: -20 } // Decrement stock by 20
  }
)


// --- 4. READ (Verify Update) ---
// Find the product again to see the new price (169.99) and stock (330)
db.products.findOne({ product_name: "Quantum Echo Speaker" })


// --- 5. DELETE ---
// Remove the product from the database
db.products.deleteOne({ product_name: "Quantum Echo Speaker" })


// --- 6. READ (Verify Delete) ---
// Try to find the product again (this should return 'null')
db.products.findOne({ product_name: "Quantum Echo Speaker" })