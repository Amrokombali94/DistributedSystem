// For Product Browsing
#pragma once

#include <unordered_map>
#include <mutex> // Mutex protects shared data from multiple threads from accessing it at once.

#include "base.hpp"
#include "models/products.hpp"

class ProductHandler : public BaseHandler
{
    public:
        ProductHandler(const std::string &basePath);
        // A Handler will be Considered as a Handler when it Implements registerRoutes method.
        void registerRoutes(App &app);

    // Create Handlers that List Products, Get Individual Product, Create Product, Update Products, and Delete Products.
    private:
        crow::response create(const crow::request &req);          // POST /api/products
        crow::response list(const crow::request &req);            // GET /api/products
        crow::response get(int id);                               // GET /api/products/<id>
        crow::response update(int id, const crow::request &req);  // PUT /api/products/<id>
        crow::response remove(int id);                            // DELETE /api/products/<id>

        // Mock Data, We will be holding the Products in Memory.
        std::unordered_map<int, Product> products_;
        int last_id_;
        std::mutex mutex_;

};