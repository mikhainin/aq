#ifndef __filter__filter__
#define __filter__filter__

#include <memory>
#include <string>

namespace filter {

namespace detail {
    struct expression_ast;
}

class Filter {
public:
    Filter(std::shared_ptr<detail::expression_ast> ast);

private:
    std::shared_ptr<detail::expression_ast> ast;
};

}

#endif