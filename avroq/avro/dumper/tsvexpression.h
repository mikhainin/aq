#pragma once

#include <unordered_map>
#include <string>

namespace avro {
namespace dumper {

struct TsvExpression {
    using map_t = std::unordered_multimap<int, int>;
    map_t what;
    int pos = 0;
    std::string fieldSeparator;
};

}
}
