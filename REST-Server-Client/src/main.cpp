#include <iostream>
#include <stdexcept>

#include "server.hpp"

int main()
{
    try
    {
        ServerConfig config;
        config.port = 8000;
        config.threads = 4;
        config.log_level = "debug";
        config.cors = true;
        config.corsOrigin = "*";

        auto server = std::make_unique<Server>(config);
        server->start();
        return 0;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return -1;
    }
    
}