
#ifndef __avroq__enum__
#define __avroq__enum__

#include <vector>
#include <string>

#include "node.h"

namespace avro {
namespace node {

class Enum : public Node {
public:
    explicit Enum(int number, const std::string &name);
    void addValue(const std::string &value);
    const std::string &operator[](int i) const;
private:
    std::vector<std::string> values;
};
}
}


#endif /* defined(__avroq__enum__) */
