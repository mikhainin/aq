
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

    // TODO: return const ref here
    std::vector<std::string> getItems() const;

    // -1 if not found
    int findIndexForValue(const std::string &value) const;
private:
    std::vector<std::string> values;
};
}
}


#endif /* defined(__avroq__enum__) */
