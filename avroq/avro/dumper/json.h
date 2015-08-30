#ifndef __avroq_avro_dumper_fool_
#define __avroq_avro_dumper_fool_

#include <ostream>

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <avro/node/all_nodes.h>
#include <avro/stringbuffer.h>

#include "tsvexpression.h"

namespace avro {
namespace dumper {

namespace JsonTag {
    struct pretty {
        using json_writer = rapidjson::PrettyWriter<rapidjson::StringBuffer>;
    };
    struct plain {
        using json_writer = rapidjson::Writer<rapidjson::StringBuffer>;
    };

}

template<class tag = JsonTag::plain>
class Json {
public:

    Json() : writer(buffer) {
    }

    void String(const avro::StringBuffer &s, const node::String &n) {
        writer.String(n.getItemName());
        writer.String(s.data(), s.bytesLeft());
    }

    void MapName(const avro::StringBuffer &name) {
        writer.String(name.data(), name.bytesLeft());
    }

    void MapValue(const avro::StringBuffer &s, const node::String &n) {
        writer.String(s.data(), s.bytesLeft());
    }

    void MapValue(int i, const node::Int &n) {
        writer.Int(i);
    }

    void Int(int i, const node::Int &n) {
        writer.String(n.getItemName());
        writer.Int(i);
    }

    void Long(long l, const node::Long &n) {
        writer.String(n.getItemName());
        writer.Int64(l);
    }

    void Float(float f, const node::Float &n) {
        writer.String(n.getItemName());
        writer.Double(f);
    }

    void Double(double d, const node::Double &n) {
        writer.String(n.getItemName());
        writer.Double(d);
    }

    void Boolean(bool b, const node::Boolean &n) {
        writer.String(n.getItemName());
        writer.Bool(b);
    }

    void Null(const node::Null &n) {
        writer.String(n.getItemName());
        writer.Null();
    }

    void RecordBegin(const node::Record &r) {
        writer.StartObject();
    }

    void RecordEnd(const node::Record &r) {
        writer.EndObject();
    }

    void ArrayBegin(const node::Array &a) {
        writer.StartArray();
    }

    void ArrayEnd(const node::Array &a) {
        writer.EndArray();
    }

    void CustomBegin(const node::Custom &c) {
        writer.String(c.getItemName());
        // outStream << indents[level] << c.getItemName();
    }

    void Enum(const node::Enum &e, int index) {
        const auto &value = e[index];
        writer.String(value);
        // outStream << ": \"" << value  << '"' << std::endl;
    }

    void MapBegin(const node::Map &m) {
        // outStream << "{\n";
        // level++;
        writer.StartObject();
    }

    void MapEnd(const node::Map &m) {
        // level--;
        // outStream << indents[level] << "}\n";
        writer.EndObject();

    }

    void EndDocument(std::function<void(const std::string &)> dumpMethod) {
        dumpMethod(buffer.GetString());
    }

private:
    rapidjson::StringBuffer buffer;
    typename tag::json_writer writer;

};

}
}


#endif