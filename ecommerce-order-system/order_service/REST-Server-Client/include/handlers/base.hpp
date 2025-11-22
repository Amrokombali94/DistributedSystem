// Implements Base Handler
#pragma once

#include <string>

#include "interface.hpp"

// An Abstract Class that Provides Common Functionality that the Real Handlers will need.
class BaseHandler : public IHandler
{
    public:
        explicit BaseHandler(const std::string &basePath);

    protected:
        // Base Path for Routing Purposes
        std::string basePath_;

        // Utility Functions
        static crow::response bad_request(const std::string &message);
        static crow::response not_found(const std::string &message);
        static crow::response internal(const std::string &message);
};