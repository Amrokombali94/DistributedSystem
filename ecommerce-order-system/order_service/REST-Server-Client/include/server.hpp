// Main Engine that Brings Everything Together.
#pragma once

#include <crow.h>
#include <memory>
#include <vector>
#include <string>

#include "handlers/interface.hpp"
#include "handlers/users.hpp"
#include "handlers/products.hpp"
#include "handlers/orders.hpp"

struct ServerConfig {
    int port = 8080;
    int threads = 2;
    std::string log_level = "info";
    bool cors = true;
    std::string corsOrigin = "*";
};

class Server
{
    public:
        explicit Server(const ServerConfig &config = ServerConfig());

        void start();

    private:
        ServerConfig config_;
        std::unique_ptr<App> app_;
        std::vector<std::shared_ptr<IHandler>> handlers_;

        // Setup Handlers
        void setup();
        void addHandler(std::shared_ptr<IHandler> handler);
};