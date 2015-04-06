cmake_minimum_required (VERSION 2.6)
project (avroq)

find_package(Boost COMPONENTS iostreams program_options)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
else()
    message( FATAL_ERROR "Unable to find Boost libraries")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -pedantic -Wall -Werror")

# include_directories ("${PROJECT_SOURCE_DIR}/avro")
add_subdirectory (avro) 

add_executable(avroq main.cc)

target_link_libraries (avroq avro ${Boost_LIBRARIES})
