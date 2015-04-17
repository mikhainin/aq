cmake_minimum_required (VERSION 2.6)
project (avroq)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost COMPONENTS program_options)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
else()
    message( FATAL_ERROR "Unable to find Boost libraries")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -pedantic -Wall -Werror -Wno-unused-local-typedefs -O3")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static -flto")

# include_directories ("${PROJECT_SOURCE_DIR}/avro")
add_subdirectory (avro) 

add_executable(avroq main.cc)

target_link_libraries (avroq avro ${Boost_LIBRARIES} z)
