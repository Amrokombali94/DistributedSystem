// For Order Placement, Tracking, and Management
#pragma once

#include <unordered_map>
#include <mutex> // Mutex protects shared data from multiple threads from accessing it at once.

#include "base.hpp"
#include "models/orders.hpp"

class OrderHandler : public BaseHandler
{
    public:
        OrderHandler(const std::string &basePath);
        // A Handler will be Considered as a Handler when it Implements registerRoutes method.
        void registerRoutes(App &app);

    // Create Handlers that List Products, Get Individual Product, Create Product, Update Products, and Delete Products.
    private:
        crow::response create(const crow::request &req);          // POST /api/orders
        crow::response list(const crow::request &req);            // GET /api/orders
        crow::response get(int id);                               // GET /api/orders/<id> (Admin Only)
        crow::response update(int id, const crow::request &req);  // PUT /api/orders/<id> (Admin Only)
        crow::response remove(int id);                            // DELETE /api/orders/<id> (Admin Only)

        // Mock Data, We will be holding the Products in Memory.
        std::unordered_map<int, Order> orders_;
        int last_id_;
        std::mutex mutex_;

};