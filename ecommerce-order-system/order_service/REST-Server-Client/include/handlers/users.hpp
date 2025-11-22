// For Registering Users
#pragma once

#include <unordered_map>
#include <mutex> // Mutex protects shared data from multiple threads from accessing it at once.

#include "base.hpp"
#include "models/users.hpp"

class UserHandler : public BaseHandler
{
    public:
        UserHandler(const std::string &basePath);
        // A Handler will be Considered as a Handler when it Implements registerRoutes method.
        void registerRoutes(App &app);

    // Create Handlers that List Products, Get Individual User, Create User, Update Users, and Delete Users.
    private:
        crow::response create(const crow::request &req);          // POST /api/users
        crow::response list(const crow::request &req);            // GET /api/users
        crow::response get(int id);                               // GET /api/users/<id>
        crow::response update(int id, const crow::request &req);  // PUT /api/users/<id>
        crow::response remove(int id);                            // DELETE /api/users/<id>

        // Mock Data, We will be holding our Users in Memory.
        std::unordered_map<int, User> users_;
        int last_id_;
        std::mutex mutex_;

};