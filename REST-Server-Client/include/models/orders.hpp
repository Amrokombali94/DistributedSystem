#pragma once

#include <string>
#include <vector>

struct Order
{
    int userId;                         // Which user placed the order
    int orderId;                        // Order's Unique Identifier Number
    std::vector<int> productIds;        // Products included in the order
    std::string address;                // Delivery Address
    std::string payment;                // Payment Method (Visa/Discover/Credit/MasterCard)
    std::string shipping;               // Order's Shipping Info (Standard/Expedited)
    std::string status;                 // Order's Status (Ordered/In Transit/ Out for Delivery/ Delivered/ Cancelled)
};
