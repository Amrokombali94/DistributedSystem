// Defines our Interface Class
#pragma once

#include <crow.h>
#include <crow/middlewares/cors.h>

using App = crow::App<crow::CORSHandler>;

// An Interface Class that our Real API Handlers must Follow.
// Real Handler Class must Implement Register Roads Methods.
class IHandler 
{
    public:
        virtual void registerRoutes(App &app) = 0;
};