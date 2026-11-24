cmake_minimum_required(VERSION 3.16)

# /usr/bin/c++ on some images is Clang without a usable libstdc++ link.
# The project is built and verified with g++.
if(NOT DEFINED CMAKE_CXX_COMPILER OR CMAKE_CXX_COMPILER STREQUAL "")
    set(CMAKE_CXX_COMPILER g++)
endif()

project(zero_copy_sensor_ipc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# shm_open, clock_gettime, sigaction, getopt_long
add_compile_definitions(_GNU_SOURCE)

add_compile_options(-O3 -Wall -Wextra -Werror -pthread)
add_link_options(-pthread)

add_executable(sensor_producer src/producer.cpp)
add_executable(sensor_consumer src/consumer.cpp)
add_executable(socket_bench src/socket_bench.cpp)

foreach(target sensor_producer sensor_consumer socket_bench)
    target_include_directories(${target} PRIVATE ${CMAKE_SOURCE_DIR}/include)
    target_link_libraries(${target} PRIVATE rt pthread)
endforeach()
