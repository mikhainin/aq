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

    void WriteName(const node::Node &n) {
        if (!n.getItemName().empty()) {
            writer.String(n.getItemName());
        }
    }
    void String(const avro::StringBuffer &s, const node::String &n) {
        WriteName(n);
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
        WriteName(n);
        writer.Int(i);
    }

    void Long(long l, const node::Long &n) {
        WriteName(n);
        writer.Int64(l);
    }

    void Float(float f, const node::Float &n) {
        WriteName(n);
        writer.Double(f);
    }

    void Double(double d, const node::Double &n) {
        WriteName(n);
        writer.Double(d);
    }

    void Boolean(bool b, const node::Boolean &n) {
        WriteName(n);
        writer.Bool(b);
    }

    void Null(const node::Null &n) {
        WriteName(n);
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
        WriteName(c);
    }

    void Enum(const node::Enum &e, int index) {
        const auto &value = e[index];
        writer.String(value);
    }

    void MapBegin(const node::Map &m) {
        writer.StartObject();
    }

    void MapEnd(const node::Map &m) {
        writer.EndObject();

    }

    void EndDocument(std::function<void(const std::string &)> dumpMethod) {
        dumpMethod(buffer.GetString());
    }

    std::string GetAsString() {
        return buffer.GetString();
    }

private:
    rapidjson::StringBuffer buffer;
    typename tag::json_writer writer;

};

}
}


#endif