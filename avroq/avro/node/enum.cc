
#include "enum.h"



namespace avro {
namespace node {


Enum::Enum(int number, const std::string &name) : Node(number, "enum", name) {
}

void Enum::addValue(const std::string &value) {
    values.push_back(value);
}


const std::string &Enum::operator[](int i) const {
    return values[i];
}

}
}