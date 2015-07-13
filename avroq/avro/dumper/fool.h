#pragma once

#include <ostream>
// #include <map>

#include <avro/node/all_nodes.h>
#include <avro/stringbuffer.h>

#include "tsvexpression.h"

namespace avro {
namespace dumper {


constexpr static const char *indents[] = {
        "",
        "\t",
        "\t\t",
        "\t\t\t",
        "\t\t\t\t",
        "\t\t\t\t\t",
        "\t\t\t\t\t\t",
        "\t\t\t\t\t\t\t",
        "\t\t\t\t\t\t\t\t",
        "\t\t\t\t\t\t\t\t\t",
        "\t\t\t\t\t\t\t\t\t\t",
        "\t\t\t\t\t\t\t\t\t\t\t",
        "\t\t\t\t\t\t\t\t\t\t\t\t",
    };

class Fool {
public:
    void String(const StringBuffer &s, const node::String &n) {
        std::cout << indents[level] << n.getItemName() << ": \"" << s << '"' << std::endl;
    }

    void MapName(const StringBuffer &name) {
        std::cout << indents[level] << name;
    }

    void MapValue(const StringBuffer &s, const node::String &n) {
        std::cout << ": \"" << s << "\"" << std::endl;
    }

    void MapValue(int i, const node::Int &n) {
        std::cout << ": " << i << std::endl;
    }

    void Int(int i, const node::Int &n) {
        std::cout << indents[level] << n.getItemName() << ':' << ' ' << i << std::endl;
    }

    void Long(long l, const node::Long &n) {
        std::cout << indents[level] << n.getItemName() << ':' << ' ' << l << std::endl;
    }

    void Float(float f, const node::Float &n) {
        std::cout << indents[level] << n.getItemName() << ':' << ' ' << f << std::endl;
    }

    void Double(double d, const node::Double &n) {
        std::cout << indents[level] << n.getItemName() << ':' << ' ' << d << std::endl;
    }

    void Boolean(bool b, const node::Boolean &n) {
        std::cout << indents[level] << n.getItemName() << ':' << ' ' << (b ? "true" : "false") << std::endl;
    }

    void Null(const node::Null &n) {
        std::cout << indents[level] << n.getItemName() << ": null" << std::endl;
    }

    void RecordBegin(const node::Record &r) {
        std::cout << "{\n";
        level++;
    }

    void RecordEnd(const node::Record &r) {
        --level;
        std::cout << indents[level] << "}\n";
    }

    void ArrayBegin(const node::Array &a) {
        std::cout << "[\n";
        level++;

    }

    void ArrayEnd(const node::Array &a) {
        level--;
        std::cout << indents[level] << "]\n";
    }

    void CustomBegin(const node::Custom &c) {
        std::cout << indents[level] << c.getItemName();
    }

    void Enum(const node::Enum &e, int index) {
        const auto &value = e[index];
        std::cout << ": \"" << value  << '"' << std::endl;
    }

    void MapBegin(const node::Map &m) {
        std::cout << "{\n";
        level++;
    }

    void MapEnd(const node::Map &m) {
        level--;
        std::cout << indents[level] << "}\n";
    }

    void EndDocument() {
    }

private:
    int level = 0;

};

}
}