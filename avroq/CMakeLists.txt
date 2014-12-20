cmake_minimum_required (VERSION 2.6)
project (avroq)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pedantic -Wall -Werror")

include_directories ("${PROJECT_SOURCE_DIR}/avro")
add_subdirectory (avro) 

add_executable(avroq main.cc)

target_link_libraries (avroq avro boost_iostreams)
