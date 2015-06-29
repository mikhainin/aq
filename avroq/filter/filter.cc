

#include "filter.h"

namespace filter {

Filter::Filter(std::shared_ptr<detail::expression_ast> ast) : ast(ast) {
}

}
