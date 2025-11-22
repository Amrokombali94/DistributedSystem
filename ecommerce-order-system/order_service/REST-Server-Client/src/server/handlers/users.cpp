#include "handlers/users.hpp"

UserHandler::UserHandler(const std::string &basePath) : BaseHandler(basePath)
{
    // Init Mock Data
    this->last_id_ = 0;

    this->last_id_ += 1;
    this->users_[this->last_id_] = User{this->last_id_, true, "Admin User", "Admin@example.com", "555-123-4567", "password"};
    
}

// Fetches all Users
crow::response UserHandler::list(const crow::request &req)
{
    // Get Search Query
    auto q = req.url_params.get("q") ?: "1";

    int page = 1;
    int limit = 10;

    auto pageParam = req.url_params.get("page") ?: "1";
    auto limitParam = req.url_params.get("limit") ?: "10";

    page = std::stoi(pageParam);
    limit = std::stoi(limitParam);

    std::cout << "Q: " << q << " " << "Page and Limit: " << page << " " << limit << "\n";

    crow::json::wvalue resp;
    crow::json::wvalue::list users;

    // Look through Users, Create a User Json, Write User Data, and Push Back the Users.
    for (auto &user : this->users_)
    {
        crow::json::wvalue userJson;
        userJson["id"] = user.second.id; // Using second since the list is a vector of pairs.
        userJson["name"] = user.second.name;
        userJson["email"] = user.second.email;

        users.push_back(std::move(userJson));
    }

    // Used std move to move instead of copy for efficiency.
    resp["users"] = std::move(users);

    return crow::response(crow::OK, resp);
}

// Gets single user by ID.
crow::response UserHandler::get(int id)
{
    // Checks if the User Exists
    if (this->users_.find(id) ==  this->users_.end())
    {
        return this->not_found("User not Found");
    }
    
    User user = this->users_[id];
    crow::json::wvalue userJson;
    userJson["id"] = user.id;
    userJson["name"] = user.name;
    userJson["email"] = user.email;

    return crow::response(crow::OK, userJson);
}

crow::response UserHandler::create(const crow::request &req)
{
    // Reading Value
    crow::json::rvalue json = crow::json::load(req.body); 

    // If the user does not provide one of the required fields, return an error message.
    if (!json.has("name") || !json.has("email") || !json.has("password")){
        return this->bad_request("Missing Required Fields: name, email, or password");
    }


    // Extract Name, Email, and Password
    std::string name = json["name"].s();
    std::string email = json["email"].s();
    std::string password = json["password"].s();

    // If there is a Phone Number, extract it, otherwise make it empty.
    std::string phoneNumber;
    if (json.has("phoneNumber")) {
        phoneNumber = json["phoneNumber"].s();
    } else {
        phoneNumber = "";
    }

    bool isAdmin = false;

    // Locks so multiple requests can't modify the data.
    this->mutex_.lock();

    // Store's the Values into our Database.
    this->last_id_ += 1;
    User user = User{this->last_id_, isAdmin, name, email, phoneNumber, password};
    this->users_[this->last_id_] = user;

    this->mutex_.unlock();

    crow::json::wvalue response;
    response["id"] = user.id;
    response["name"] = user.name;
    response["email"] = user.email;
    response["phoneNumber"] = user.phoneNumber;
    response["isAdmin"] = user.isAdmin;


    return crow::response(crow::CREATED, response);
}

crow::response UserHandler::update(int id, const crow::request &req)
{
    // Checks if the User Exists
    if (this->users_.find(id) ==  this->users_.end())
    {
        return this->not_found("User not Found");
    }

     crow::json::rvalue json = crow::json::load(req.body); // Reading Value

    // Extract Name & Email
    std::string name = json["name"].s();
    std::string email = json["email"].s();

    // Locks so multiple requests can't modify the data.
    this->mutex_.lock();

    // Update User
    User user = this->users_[id];
    user.name = name;
    user.email = email;

    this->users_[id] = user;

    this->mutex_.unlock();

    crow::json::wvalue response;
    response["id"] = user.id;
    response["name"] = user.name;
    response["email"] = user.email;

    return crow::response(crow::OK, response);
}

crow::response UserHandler::remove(int id)
{
    // Checks if the User Exists
    if (this->users_.find(id) ==  this->users_.end())
    {
        return this->not_found("User not Found");
    }

    this->mutex_.lock();

    // Erases User
    this->users_.erase(id);

    this->mutex_.unlock();
    
    crow::json::wvalue response;
    response["success"] = true;

    return crow::response(crow::OK, response);
}

void UserHandler::registerRoutes(App &app)
{
    // Searches and call the Handler for that Enpdpoint/Method.

    /* GET, Lists all the Users
    app.route_dynamic(this->basePath_)
        .methods(crow::HTTPMethod::GET)(
            [this](const crow::request &req)
            {
                return this->list(req);
            });*/
    
    /* GET, Lists an individual User
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::GET)(
            [this](const crow::request &req, int id)
            {
                return this->get(id);
            });*/

    // POST, Creates a User
    app.route_dynamic(this->basePath_ + "")
        .methods(crow::HTTPMethod::POST)(
            [this](const crow::request &req)
            {
                return this->create(req);
            });
    
    /* PUT, Update a User
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::PUT)(
            [this](const crow::request &req, int id)
            {
                return this->update(id, req);
            });*/
    
    /* DELETE, Remove a User
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::DELETE)(
            [this](const crow::request &req, int id)
            {
                return this->remove(id);
            });*/
}