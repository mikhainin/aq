#pragma once

#include <ostream>

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
        outStream << indents[level] << n.getItemName() << ": \"" << s << '"' << std::endl;
    }

    void MapName(const StringBuffer &name) {
        outStream << indents[level] << name;
    }

    void MapValue(const StringBuffer &s, const node::String &n) {
        outStream << ": \"" << s << "\"" << std::endl;
    }

    void MapValue(int i, const node::Int &n) {
        outStream << ": " << i << std::endl;
    }

    void Int(int i, const node::Int &n) {
        outStream << indents[level] << n.getItemName() << ':' << ' ' << i << std::endl;
    }

    void Long(long l, const node::Long &n) {
        outStream << indents[level] << n.getItemName() << ':' << ' ' << l << std::endl;
    }

    void Float(float f, const node::Float &n) {
        outStream << indents[level] << n.getItemName() << ':' << ' ' << f << std::endl;
    }

    void Double(double d, const node::Double &n) {
        outStream << indents[level] << n.getItemName() << ':' << ' ' << d << std::endl;
    }

    void Boolean(bool b, const node::Boolean &n) {
        outStream << indents[level] << n.getItemName() << ':' << ' ' << (b ? "true" : "false") << std::endl;
    }

    void Null(const node::Null &n) {
        outStream << indents[level] << n.getItemName() << ": null" << std::endl;
    }

    void RecordBegin(const node::Record &r) {
        outStream << "{\n";
        level++;
    }

    void RecordEnd(const node::Record &r) {
        --level;
        outStream << indents[level] << "}\n";
    }

    void ArrayBegin(const node::Array &a) {
        outStream << "[\n";
        level++;

    }

    void ArrayEnd(const node::Array &a) {
        level--;
        outStream << indents[level] << "]\n";
    }

    void CustomBegin(const node::Custom &c) {
        outStream << indents[level] << c.getItemName();
    }

    void Enum(const node::Enum &e, int index) {
        const auto &value = e[index];
        outStream << ": \"" << value  << '"' << std::endl;
    }

    void MapBegin(const node::Map &m) {
        outStream << "{\n";
        level++;
    }

    void MapEnd(const node::Map &m) {
        level--;
        outStream << indents[level] << "}\n";
    }

    void EndDocument(std::function<void(const std::string &)> dumpMethod) {
        dumpMethod(outStream.str());
    }

private:
    int level = 0;
    std::ostringstream outStream;

};

}
}
