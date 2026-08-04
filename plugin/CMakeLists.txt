# Set up the project.
cmake_minimum_required(VERSION 3.12)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
project( "RubinPlugin" )

# Disable annoying warnings
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")

# Find and set the source file.
include_directories(include/)
file(GLOB SOURCES src/*.cpp)

# Set up the library.
add_library(RubinPlugin SHARED ${SOURCES})

# Find SimGrid, CGSim, Boost, and FSMod
find_package(SimGrid REQUIRED)
find_package(Boost REQUIRED)
find_package(CGSim REQUIRED)
find_package(SQLite3   REQUIRED)
find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp)

if(NOT NLOHMANN_JSON_INCLUDE_DIR)
    message(FATAL_ERROR
        "nlohmann/json.hpp was not found; set NLOHMANN_JSON_INCLUDE_DIR")
endif()


#Link libraries and Include Directories
target_link_libraries ( RubinPlugin PUBLIC ${SimGrid_LIBRARY} ${Boost_LIBRARIES} CGSim::CGSim SQLite::SQLite3)
target_include_directories( RubinPlugin PUBLIC ${SimGrid_INCLUDE_DIR} ${CGSim_INCLUDE_DIR} ${Boost_INCLUDE_DIR} ${NLOHMANN_JSON_INCLUDE_DIR})

