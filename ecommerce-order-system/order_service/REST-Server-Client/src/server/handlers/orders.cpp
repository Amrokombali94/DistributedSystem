#include "handlers/orders.hpp"

OrderHandler::OrderHandler(const std::string &basePath) : BaseHandler(basePath)
{
    // Init Mock Data
    this->last_id_ = 0;
}

// Fetches all Orders
crow::response OrderHandler::list(const crow::request &req)
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
    crow::json::wvalue::list orders;

    // Look through Orders, Create a Order Json, Write Order Data, and Push Back the Orders.
    for (auto &pair : this->orders_)
    {
        const Order &order = pair.second;
        crow::json::wvalue orderJson;

        crow::json::wvalue::list productList;
        for (int pid : order.productIds) {
            productList.push_back(pid);
        }

        orderJson["userId"] = order.userId;
        orderJson["orderId"] = order.orderId;
        orderJson["productIds"] = std::move(productList);
        orderJson["status"] = order.status;

        orders.push_back(std::move(orderJson));
    }


    // Used std move to move instead of copy for efficiency.
    resp["orders"] = std::move(orders);

    return crow::response(crow::OK, resp);
}

// Gets single Order by ID.
crow::response OrderHandler::get(int id)
{
    // Checks if the Order Exists
    if (this->orders_.find(id) ==  this->orders_.end())
    {
        return this->not_found("Order not Found");
    }

    Order order = this->orders_[id];
    
    crow::json::wvalue::list productList;
    for (int pid : order.productIds) {
        productList.push_back(pid);
    }

    crow::json::wvalue orderJson;
    orderJson["orderId"] = order.orderId; // Using second since the list is a vector of pairs.
    orderJson["userId"] = order.userId;
    orderJson["productIds"] = std::move(productList);
    orderJson["status"] = order.status;
    orderJson["address"] = order.address;

    return crow::response(crow::OK, orderJson);
}

// Places an Order
crow::response OrderHandler::create(const crow::request &req)
{
    // Reading Value
    crow::json::rvalue json = crow::json::load(req.body); 

    // If the user does not provide one of the required fields, return an error message.
    if (!json.has("userId") || !json.has("productIds") || !json.has("address") || !json.has("payment") || !json.has("shipping")){
        return this->bad_request("Missing Required Fields: userId, productIds, address, payment, or shipping");
    }

    // Extract UserID, ProductIDs, Address, Payment, and Shipping.
    int userId = json["userId"].i();
    std::vector<int> productIds;
    for (size_t i = 0; i < json["productIds"].size(); i++) {
        productIds.push_back(json["productIds"][i].i()); // .i() = integer
    }
    std::string address = json["address"].s();
    std::string payment = json["payment"].s();
    std::string shipping = json["shipping"].s();

    // Locks so multiple requests can't modify the data.
    this->mutex_.lock();

    // Store's the Values into our Database.
    this->last_id_ += 1;
    Order order = Order{userId, last_id_, productIds, address, payment, shipping, "Ordered"};
    this->orders_[this->last_id_] = order;

    this->mutex_.unlock();

    crow::json::wvalue::list productList;
    for (int pid : order.productIds) {
        productList.push_back(pid);
    }


    crow::json::wvalue response;
    response["userId"] = order.userId;
    response["orderId"] = order.orderId;
    response["productIds"] = std::move(productList);
    response["address"] = order.address;
    response["payment"] = order.payment;
    response["shipping"] = order.shipping;


    return crow::response(crow::CREATED, response);
}

crow::response OrderHandler::update(int id, const crow::request &req)
{
    // Checks if the Order Exists
    if (this->orders_.find(id) ==  this->orders_.end())
    {
        return this->not_found("Order not Found");
    }

    crow::json::rvalue json = crow::json::load(req.body); // Reading Value

    // Extract Status.
    std::string status = json["status"].s();

    // Locks so multiple requests can't modify the data.
    this->mutex_.lock();

    // Update Order
    Order order = this->orders_[id];
    order.status = status;

    this->orders_[id] = order;

    this->mutex_.unlock();

    crow::json::wvalue response;
    response["orderId"] = order.orderId;
    response["status"] = order.status;

    return crow::response(crow::OK, response);
}

crow::response OrderHandler::remove(int id)
{
    // Checks if the Order Exists
    if (this->orders_.find(id) ==  this->orders_.end())
    {
        return this->not_found("Order not Found");
    }

    this->mutex_.lock();

    // Erases Order
    Order order = this->orders_[id];
    this->orders_.erase(id);

    this->mutex_.unlock();
    
    crow::json::wvalue response;
    response["orderId"] = order.orderId;
    response["userId"] = order.userId;
    response["success"] = true;


    return crow::response(crow::OK, response);
}

void OrderHandler::registerRoutes(App &app)
{
    // Searches and call the Handler for that Enpdpoint/Method.

    // GET, Lists all the Orders (ADMIN Only)
    app.route_dynamic(this->basePath_ + "")
        .methods(crow::HTTPMethod::GET)(
            [this](const crow::request &req)
            {
                return this->list(req);
            });
    
    // GET, Lists an individual Order
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::GET)(
            [this](const crow::request &req, int id)
            {
                return this->get(id);
            });

    // POST, Places an Order
    app.route_dynamic(this->basePath_ + "")
        .methods(crow::HTTPMethod::POST)(
            [this](const crow::request &req)
            {
                return this->create(req);
            });
    
    // PUT, Update an Order (ADMIN Only)
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::PUT)(
            [this](const crow::request &req, int id)
            {
                return this->update(id, req);
            });
    
    // DELETE, Cancel an Order (ADMIN Only)
    app.route_dynamic(this->basePath_ + "/<int>")
        .methods(crow::HTTPMethod::DELETE)(
            [this](const crow::request &req, int id)
            {
                return this->remove(id);
            });
}