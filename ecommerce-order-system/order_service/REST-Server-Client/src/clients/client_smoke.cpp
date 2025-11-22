#include <curl/curl.h>
#include <iostream>
#include <string>
#include <regex>

static size_t write_cb(void* ptr, size_t size, size_t nmemb, void* userdata){
    auto* s = static_cast<std::string*>(userdata);
    s->append(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

std::string request(const std::string& url,
                    const char* method = "GET",
                    const std::string& body = "",
                    const std::vector<std::string>& extraHeaders = {}) {
    CURL* curl = curl_easy_init();
    std::string resp;
    if (!curl) return resp;

    struct curl_slist* headers = nullptr;
    for (auto& h : extraHeaders) headers = curl_slist_append(headers, h.c_str());
    if (!body.empty()) headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    if (!body.empty()) curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    if (headers)       curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) std::cerr << method << " " << url << " failed: "
                                   << curl_easy_strerror(res) << "\n";
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return resp;
}

int extract_number_field(const std::string& json, const std::string& field) {
    std::regex rx("\"" + field + "\"\\s*:\\s*([0-9]+)");
    std::smatch m; if (std::regex_search(json, m, rx)) return std::stoi(m[1].str());
    return -1;
}

int main(int argc, char** argv) {
    std::string base = (argc > 1) ? argv[1] : "http://localhost:8080";

    // 1) Product browsing
    std::cout << "== List products ==\n";
    std::string products = request(base + "/api/products");
    std::cout << products << "\n";

    // (optional) product detail 1
    std::cout << "== Product detail (id=1) ==\n";
    std::cout << request(base + "/api/products/1") << "\n";

    // 2) User registration
    std::cout << "== Register user ==\n";
    std::string ujson = R"({"name":"CppUser","email":"cpp@example.com","password":"pass"})";
    std::string uresp = request(base + "/api/users", "POST", ujson);
    std::cout << uresp << "\n";
    int userId = extract_number_field(uresp, "id");
    if (userId < 0) userId = 1; // fallback for demo
    std::cout << "userId=" << userId << "\n";

    // 3) Order placement
    std::cout << "== Place order ==\n";
    std::string ojson = std::string("{\"userId\":") + std::to_string(userId) +
        ",\"productIds\":[1,2],\"address\":\"123 Demo St\",\"payment\":\"card\",\"shipping\":\"std\"}";
    std::string oresp = request(base + "/api/orders", "POST", ojson);
    std::cout << oresp << "\n";
    int orderId = extract_number_field(oresp, "orderId");
    if (orderId < 0) orderId = 1; // fallback

    // 4) Order tracking (by user + by id)
    std::cout << "== My orders (filter) ==\n";
    std::cout << request(base + "/api/orders?userId=" + std::to_string(userId)) << "\n";

    std::cout << "== Get order by id ==\n";
    std::cout << request(base + "/api/orders/" + std::to_string(orderId)) << "\n";

    // 5) Order management (admin): update status + cancel
    std::cout << "== Admin: update status -> processed ==\n";
    std::cout << request(base + "/api/orders/" + std::to_string(orderId),
                         "PUT", R"({"status":"processed"})") << "\n";

    std::cout << "== Admin: cancel order ==\n";
    std::cout << request(base + "/api/orders/" + std::to_string(orderId),
                         "DELETE") << "\n";

    std::cout << "Done.\n";
    return 0;
}
