// The Base Handler Serves as a Foundation for all of the API EndPoints and Handlers.

#include "handlers/base.hpp"

// Initalizes the Member Variable
BaseHandler::BaseHandler(const std::string &basePath) : basePath_(basePath) {}

crow::response BaseHandler::bad_request(const std::string &message)
{
    // Writes the values to this JSon
    crow::json::wvalue resp;

    resp["status"] = "error";
    resp["message"] = message;

    return crow::response(crow::BAD_REQUEST, resp);
}

crow::response BaseHandler::not_found(const std::string &message)
{
    // Writes the values to this JSon
    crow::json::wvalue resp;

    resp["status"] = "error";
    resp["message"] = message;

    return crow::response(crow::NOT_FOUND, resp);
}

crow::response BaseHandler::internal(const std::string &message)
{
    // Writes the values to this JSon
    crow::json::wvalue resp;

    resp["status"] = "error";
    resp["message"] = message;

    return crow::response(crow::INTERNAL_SERVER_ERROR, resp);
}