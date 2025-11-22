#include "server.hpp"

Server::Server(const ServerConfig &config) : config_(config)
{
    // Set Up Logging Level
    if (this->config_.log_level == "debug")
    {
        crow::logger::setLogLevel(crow::LogLevel::DEBUG);
    }
    else if (this->config_.log_level == "info")
    {
        crow::logger::setLogLevel(crow::LogLevel::INFO);
    }
    else if (this->config_.log_level == "warning")
    {
        crow::logger::setLogLevel(crow::LogLevel::WARNING);
    }
    else if (this->config_.log_level == "error")
    {
        crow::logger::setLogLevel(crow::LogLevel::ERROR);
    }
    else if (this->config_.log_level == "critical")
    {
        crow::logger::setLogLevel(crow::LogLevel::CRITICAL);
    }
    else
    {
        crow::logger::setLogLevel(crow::LogLevel::INFO);
    }

    // App Init
    this->app_ = std::make_unique<App>();

    // Config Cors, crucial for Browser Security.
    if (this->config_.cors)
    {
        auto &cors = this->app_->get_middleware<crow::CORSHandler>();

        cors
            .global()
            // Defines what HTTP methods are allowed.
            .methods(crow::HTTPMethod::GET, crow::HTTPMethod::POST, crow::HTTPMethod::PUT, crow::HTTPMethod::DELETE, crow::HTTPMethod::OPTIONS)
            .headers("Content-Type", "Authorization")
            .origin(this->config_.corsOrigin)
            .prefix("/api")
            .max_age(3600);
    }

}

void Server::setup()
{
    this->addHandler(std::make_shared<UserHandler>("/api/users"));
    this->addHandler(std::make_shared<ProductHandler>("/api/products"));
    this->addHandler(std::make_shared<OrderHandler>("/api/orders"));
}

void Server::addHandler(std::shared_ptr<IHandler> handler)
{
    // Push Back to the Vector to our New Handler Reference.
    this->handlers_.push_back(handler);

    // Call Register Routes Method so that it Register all the Routes and the Handlers it has to the Application.
    handler->registerRoutes(*this->app_);
}

void Server::start()
{
    // Calls setup to Register all Handlers.
    this->setup();

    // Specifies the Port from the Configuration.
    // Calls the multithreaded method that tells the Server it's going to be run in Multi-Threaded Mode.
    // The Concurrent Method tells the Server how many Threads it should run.
    // The Run Async Method launches the Crow Server.
    this->app_->port(this->config_.port).multithreaded().concurrency(this->config_.threads).run_async();
}
