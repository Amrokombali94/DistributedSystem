#include "handlers/products.hpp"

ProductHandler::ProductHandler(const std::string &basePath) : BaseHandler(basePath)
{
    // Init Mock Data
    this->last_id_ = 0;

    this->last_id_ += 1;
    this->products_[this->last_id_] = Product{this->last_id_, 999.99, "MacBook Air", "New: 10-Core CPI, 8-Core GPU, 16GB RAM, 256GB SSD Storage", "MacBook.png"};
    
    this->last_id_ += 1;
    this->products_[this->last_id_] = Product{this->last_id_, 199.99, "AirPods Pro 3", "New: Active Noise Cancellation, Conversation Awareness, Personalized Volume, Loud Sound Reduction", "AirPods.png"};

    this->last_id_ += 1;
    this->products_[this->last_id_] = Product{this->last_id_, 799.99, "iPhone 17", "New: A19 Chip, 6-Core CPU, 5-Core GPU", "iPhone.png"};

}

// Fetches all Products
crow::response ProductHandler::list(const crow::request &req)
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
    crow::json::wvalue::list products;

    // Look through Products, Create a Product Json, Write Product Data, and Push Back the Products.
    for (auto &product : this->products_)
    {
        crow::json::wvalue productJson;
        productJson["id"] = product.second.id; // Using second since the list is a vector of pairs.

        // Format for display (2 decimal places, string)
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << product.second.price;
        productJson["price"] = oss.str();

        productJson["name"] = product.second.name;
        productJson["description"] = product.second.description;
        productJson["image"] = product.second.image;

        products.push_back(std::move(productJson));
    }

    // Used std move to move instead of copy for efficiency.
    resp["products"] = std::move(products);

    return crow::response(crow::OK, resp);
}

// Gets single Product by ID.
crow::response ProductHandler::get(int id)
{
    // Checks if the Product Exists
    if (this->products_.find(id) ==  this->products_.end())
    {
        return this->not_found("Product not Found");
    }
    
    Product product = this->products_[id];
    crow::json::wvalue productJson;
    productJson["id"] = product.id; // Using second since the list is a vector of pairs.
    productJson["price"] = product.price;
    productJson["name"] = product.name;
    productJson["description"] = product.description;
    productJson["image"] = product.image;

    return crow::response(crow::OK, productJson);
}

crow::response ProductHandler::create(const crow::request &req)
{
    // Reading Value
    crow::json::rvalue json = crow::json::load(req.body); 

    // If the user does not provide one of the required fields, return an error message.
    if (!json.has("price") || !json.has("name") || !json.has("description") || !json.has("image")){
        return this->bad_request("Missing Required Fields: name, description, price, or image");
    }

    // Extract Name, Description, Price, and Image.
    double price = json["price"].d();
    std::string name = json["name"].s();
    std::string description = json["description"].s();
    std::string image = json["image"].s();

    // Locks so multiple requests can't modify the data.
    this->mutex_.lock();

    // Store's the Values into our Database.
    this->last_id_ += 1;
    Product product = Product{this->last_id_, price, name, description, image};
    this->products_[this->last_id_] = product;

    this->mutex_.unlock();

    crow::json::wvalue response;
    response["id"] = product.id;
    response["name"] = product.name;
    response["description"] = product.description;
    response["price"] = product.price;
    response["image"] = product.image;


    return crow::response(crow::CREATED, response);
}

crow::response ProductHandler::update(int id, const crow::request &req)
{
    // Checks if the Product Exists
    if (this->products_.find(id) ==  this->products_.end())
    {
        return this->not_found("Product not Found");
    }

    crow::json::rvalue json = crow::json::load(req.body); // Reading Value

    // Extract Name, Description, Price, and Image.
    double price = json["price"].d();
    std::string name = json["name"].s();
    std::string description = json["description"].s();
    std::string image = json["image"].s();

    // Locks so multiple requests can't modify the data.
    this->mutex_.lock();

    // Update Product
    Product product = this->products_[id];
    product.name = name;
    product.description = description;
    product.price = price;
    product.image = image;

    this->products_[id] = product;

    this->mutex_.unlock();

    crow::json::wvalue response;
    response["id"] = product.id;
    response["name"] = product.name;
    response["description"] = product.description;

    // Format for display (2 decimal places, string)
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << product.price;
    response["price"] = oss.str();

    response["image"] = product.image;

    return crow::response(crow::OK, response);
}

crow::response ProductHandler::remove(int id)
{
    // Checks if the Product Exists
    if (this->products_.find(id) ==  this->products_.end())
    {
        return this->not_found("Product not Found");
    }

    this->mutex_.lock();

    // Erases Product
    this->products_.erase(id);

    this->mutex_.unlock();
    
    crow::json::wvalue response;
    response["success"] = true;

    return crow::response(crow::OK, response);
}

void ProductHandler::registerRoutes(App &app)
{
    // Searches and call the Handler for that Enpdpoint/Method.

    // GET, Lists all the Products
    app.route_dynamic(this->basePath_ + "")
        .methods(crow::HTTPMethod::GET)(
            [this](const crow::request &req)
            {
                return this->list(req);
            });
    
    // GET, Lists an individual Product
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::GET)(
            [this](const crow::request &req, int id)
            {
                return this->get(id);
            });

  /* POST, Creates a Product
    app.route_dynamic(this->basePath_)
        .methods(crow::HTTPMethod::POST)(
            [this](const crow::request &req)
            {
                return this->create(req);
            });*/
    
    /* PUT, Update a Product
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::PUT)(
            [this](const crow::request &req, int id)
            {
                return this->update(id, req);
            });*/
    
    /* DELETE, Remove a Product
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::DELETE)(
            [this](const crow::request &req, int id)
            {
                return this->remove(id);
            });*/
}