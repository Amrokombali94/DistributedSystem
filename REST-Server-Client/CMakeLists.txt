cmake_minimum_required(VERSION 3.20)
project(rest-api CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Crow (header-only)
include(FetchContent)
FetchContent_Declare(
  crow
  GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
  GIT_TAG v1.1.0
)
FetchContent_MakeAvailable(crow)  # Crow::Crow

# ---- Server target ----
add_executable(rest-api
  src/main.cpp
  src/server/server.cpp
  src/server/handlers/base.cpp
  src/server/handlers/users.cpp
  src/server/handlers/products.cpp
  src/server/handlers/orders.cpp
)
target_link_libraries(rest-api PRIVATE Crow::Crow pthread)
target_include_directories(rest-api PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_compile_features(rest-api PRIVATE cxx_std_17)

# ---- Client smoke target ----
find_package(CURL REQUIRED)

add_executable(client_smoke
  src/clients/client_smoke.cpp
)
target_link_libraries(client_smoke PRIVATE CURL::libcurl)
target_compile_features(client_smoke PRIVATE cxx_std_17)
