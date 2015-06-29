#ifndef ____compiler__
#define ____compiler__

#include <memory>
#include <string>

namespace filter {

class Filter;

class Compiler {
public:

    std::shared_ptr<Filter> compile(const std::string &str);

};

}

#endif /* defined(____compiler__) */
