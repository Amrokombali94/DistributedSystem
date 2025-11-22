#pragma once

#include <string>

struct User
{
    int id;                     // User's Unique Identifier Number
    bool isAdmin = false;       // isAdmin
    std::string name;           // User's Name
    std::string email;          // User's Email
    std::string phoneNumber;    // User's Phone Number
    std::string password;       // User's Password
};
