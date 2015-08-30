cmake_minimum_required (VERSION 2.6)
project (avroq)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost COMPONENTS program_options)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
else()
    message( FATAL_ERROR "Unable to find Boost libraries")
endif()

if (NOT APPLE)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static")
endif()

# get version from changelog

file(STRINGS ${PROJECT_SOURCE_DIR}/../debian/changelog LOGLINE LIMIT_COUNT 1)

string(REGEX MATCH "\\(.*\\)" VERSION ${LOGLINE})
string(REGEX REPLACE "[()]" "" VERSION ${VERSION})
set_source_files_properties(version.cc PROPERTIES COMPILE_FLAGS -DVERSION='"${VERSION}"')


#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -pedantic -Wall -Werror -ggdb3 -gdwarf-2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -pedantic -Wall -Werror -g -O3 ")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto ")

include_directories(${PROJECT_SOURCE_DIR})
include_directories("${PROJECT_SOURCE_DIR}/../3rdparty/rapidjson/include")

add_definitions(-DRAPIDJSON_HAS_STDSTRING)

if(CMAKE_COMPILER_IS_GNUCC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs -D_GLIBCXX_HAS_GTHREADS=1 -pthread")

# bug in GCC 4.9.2 when using static linking & threads
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}  -pthread -Wl,--no-as-needed -Wl,--whole-archive -lpthread -Wl,--no-whole-archive")
endif()

add_subdirectory (avro) 
add_subdirectory (filter)

add_executable(aq
    main.cc
    fileemitor.cc
    worker.cc
    version.cc
    )

target_link_libraries (aq avro filter ${Boost_LIBRARIES} z)
