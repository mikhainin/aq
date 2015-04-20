#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <iostream>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include <boost/algorithm/string.hpp>

#include <boost/lexical_cast.hpp>

#include "schemareader.h"
#include "limiter.h"

#include "node/node.h"
#include "node/record.h"
#include "node/union.h"
#include "node/string.h"
#include "node/custom.h"
#include "node/int.h"
#include "node/long.h"
#include "node/array.h"
#include "node/boolean.h"
#include "node/enum.h"
#include "node/float.h"
#include "node/double.h"
#include "node/map.h"
#include "node/null.h"

#include "deflatedbuffer.h"
#include "filehandler.h"
#include "stringbuffer.h"
#include "zigzag.hpp"
#include "reader.h"

namespace {
const std::string AVRO_MAGICK = "Obj\001"; // 4 bytes
}

namespace avro {

class Reader::Private {
public:
    std::string filename; // TODO: move to FileHandle

    std::unique_ptr<StringBuffer> input;

    DeflatedBuffer deflate_buffer;
    Limiter &limit;

    FileHandle file;

    Private(const std::string& filename, Limiter &limit) : filename(filename), limit(limit), file(filename) {
    }
};

Reader::Reader(const std::string& filename, Limiter &limit) :
    d(new Private(filename, limit)) {

    d->input = d->file.mmapFile();
}

Reader::~Reader() {
    delete d;
}


header Reader::readHeader() {

    
    std::string magick;
    magick.resize(AVRO_MAGICK.size());
    d->input->read(&magick[0], AVRO_MAGICK.size());
    
    if (magick != AVRO_MAGICK) {
        throw std::runtime_error("Bad avro file (wrong magick)");
    }

    header header;


    int64_t recordsNumber = readZigZagLong(*d->input);
    
    for(uint i = 0; i < recordsNumber; ++i) {

        size_t len = readZigZagLong(*d->input);
        const std::string &key = d->input->getStdString(len);
        const std::string &value = d->input->getStdString(readZigZagLong(*d->input));

        header.metadata[key] = value;

    }

    SchemaReader schemaReader(header.metadata["avro.schema"]);
    header.schema = schemaReader.parse();
    header.nodesNumber = schemaReader.nodesNumber();
    //dumpShema(schemaRoot);
    
    char c = d->input->getChar();

    assert(c == 0); // TODO: what is it?
    d->input->read(&header.sync[0], sizeof header.sync);


    return header;

}
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

class DocumentDumper {
public:
    void String(const StringBuffer &s, const node::String &n) {
        std::cout << indents[level] << n.getItemName() << ": \"" << s << '"' << std::endl;
    }

    void MapName(const StringBuffer &s, const node::String &n) {
        std::cout << indents[level] << s;
    }

    void MapValue(const StringBuffer &s, const node::String &n) {
        std::cout << ": \"" << s  << "\"" << std::endl;
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
        std::cout << indents[level] << n.getItemName() << ':' << ' ' << b << std::endl;
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

private:
    int level = 0;

};

class Dumper {
public:
    virtual ~Dumper() {}
    virtual void dump(std::ostream &os) = 0;
};

template<typename T>
class TDumper : public Dumper {
public:

    TDumper(const T &t) : Dumper(), t(t) {
    }
    void dump(std::ostream &os) {
        os << t;
    }
private:
    T t;
};

class TsvDumper {
public:
    TsvDumper(const std::map<int, int> &whatDump) : whatDump(whatDump) {
    }

    void String(const StringBuffer &s, const node::String &n) {
        toDump.emplace_back(std::unique_ptr<Dumper>(new TDumper<StringBuffer>(s)));
    }

    void MapName(const StringBuffer &s, const node::String &n) {
        // std::cout << indents[level] << s;
    }

    void MapValue(const StringBuffer &s, const node::String &n) {
        //std::cout << ": \"" << s  << "\"" << std::endl;
    }

    void Int(int i, const node::Int &n) {
        //std::cout << indents[level] << n.getItemName() << ':' << ' ' << i << std::endl;
    }

    void Long(long l, const node::Long &n) {
        //std::cout << indents[level] << n.getItemName() << ':' << ' ' << l << std::endl;
    }

    void Float(float f, const node::Float &n) {
        //std::cout << indents[level] << n.getItemName() << ':' << ' ' << f << std::endl;
    }

    void Double(double d, const node::Double &n) {
        //std::cout << indents[level] << n.getItemName() << ':' << ' ' << d << std::endl;
    }

    void Boolean(bool b, const node::Boolean &n) {
        //std::cout << indents[level] << n.getItemName() << ':' << ' ' << b << std::endl;
    }

    void Null(const node::Null &n) {
        //std::cout << indents[level] << n.getItemName() << ": null" << std::endl;
    }

    void RecordBegin(const node::Record &r) {
        //std::cout << "{\n";
        //level++;
    }

    void RecordEnd(const node::Record &r) {
        //--level;
        //std::cout << indents[level] << "}\n";
    }

    void ArrayBegin(const node::Array &a) {
        //std::cout << "[\n";
        //level++;

    }

    void ArrayEnd(const node::Array &a) {
        //level--;
        //std::cout << indents[level] << "]\n";
    }

    void CustomBegin(const node::Custom &c) {
        //std::cout << indents[level] << c.getItemName();
    }

    void Enum(const node::Enum &e, int index) {
        //const auto &value = e[index];
        //std::cout << ": \"" << value  << '"' << std::endl;
    }

    void MapBegin(const node::Map &m) {
        //std::cout << "{\n";
        //level++;
    }

    void MapEnd(const node::Map &m) {
        //level--;
        //std::cout << indents[level] << "}\n";
    }

    void EndDocument() {

    }

private:
    std::vector<std::unique_ptr<Dumper>> toDump;
    std::map<int, int> whatDump;
};




void Reader::readBlock(const header &header, const FilterExpression *filter, std::map<int, int> wd) {

    int64_t objectCountInBlock = readZigZagLong(*d->input);
    int64_t blockBytesNum = readZigZagLong(*d->input);

    if (header.metadata.at("avro.codec") == "deflate") { // TODO: check it once

        d->deflate_buffer.assignData(d->input->getAndSkip(blockBytesNum), blockBytesNum);

        for(int i = 0; i < objectCountInBlock; ++i) {
            // TODO: rewrite it using hierarcy of filters/decoders.
            // TODO: implement counter as a filter  
            if (!filter) {
                // dumpDocument(d->deflate_buffer, header.schema, 0);
                if (wd.size() > 0) {
                    auto dumper = TsvDumper(wd);
                    writeDocument(d->deflate_buffer, header.schema, dumper);
                } else {
                    auto dumper = DocumentDumper();
                    writeDocument(d->deflate_buffer, header.schema, dumper);
                }
                auto dumper = DocumentDumper();
                writeDocument(d->deflate_buffer, header.schema, dumper);
                d->limit.documentFinished();
            } else {
                try {
                    d->deflate_buffer.startDocument();
                    decodeBlock(d->deflate_buffer, header.schema, filter);
                } catch (const DumpObject &e) {
                    d->deflate_buffer.resetToDocument();
                    dumpDocument(d->deflate_buffer, header.schema, 0);
                    d->limit.documentFinished();
                }
            }
        }

        char tmp_sync[16] = {0}; // TODO sync length to a constant
        d->input->read(&tmp_sync[0], sizeof tmp_sync); // TODO: move to a function

        if (std::memcmp(tmp_sync, header.sync, sizeof tmp_sync) != 0) {
            throw std::runtime_error("Sync match failed");
        }
        
    } else {
        throw std::runtime_error("Unknown codec: " + header.metadata.at("avro.codec"));
    }

}

void Reader::decodeBlock(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, const FilterExpression *filter) {
    if (schema->is<node::Record>()) {
        for(auto &p : schema->as<node::Record>().getChildren()) {
            decodeBlock(stream, p, filter);
        }
    } else if (schema->is<node::Union>()) {
        int item = readZigZagLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        decodeBlock(stream, node, filter);
    } else if (schema->is<node::Custom>()) {
        decodeBlock(stream, schema->as<node::Custom>().getDefinition(), filter);
    } else if (schema->is<node::Enum>()) {
        int index = readZigZagLong(stream);
        (void)index;
    } else if (schema->is<node::Array>()) {
        auto const &node = schema->as<node::Array>().getItemsType();

        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                decodeBlock(stream, node, filter);
            }
        } while(objectsInBlock != 0);

    } else if (schema->is<node::Map>()) {
        auto const &node = schema->as<node::Map>().getItemsType();

        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                readString(stream);
                decodeBlock(stream, node, filter);
            }
        } while(objectsInBlock != 0);
    } else {
        if (schema->is<node::String>()) {
            const std::string value = readString(stream);
            if (filter && schema.get() == filter->shemaItem && value == filter->strValue) {
                // dump ducument
                throw DumpObject();
            }
        } else if (schema->is<node::Int>()) {
            int value = readZigZagLong(stream);

            if (filter && schema.get() == filter->shemaItem && value == filter->value.i) {
                // dump ducument
                throw DumpObject();
            }
        } else if (schema->is<node::Long>()) {
            int value = readZigZagLong(stream);

            if (filter && schema.get() == filter->shemaItem && value == filter->value.i) {
                // dump ducument
                throw DumpObject();
            }
        } else if (schema->is<node::Float>()) {
            readFloat(stream);
        } else if (schema->is<node::Double>()) {
            readDouble(stream);
        } else if (schema->is<node::Boolean>()) {
            readBoolean(stream);
        } else if (schema->is<node::Null>()) {
            ; // empty value: no way to process
        } else {
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }
    }
}
template <class T>
void Reader::writeDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, T &dumper) {
    if (schema->is<node::Record>()) {

        auto &r = schema->as<node::Record>();
        dumper.RecordBegin(r);
        for(auto &p : schema->as<node::Record>().getChildren()) {
            writeDocument(stream, p, dumper);
        }
        dumper.RecordEnd(r);

    } else if (schema->is<node::Union>()) {
        int item = readZigZagLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        writeDocument(stream, node, dumper);
    } else if (schema->is<node::Custom>()) {
        dumper.CustomBegin(schema->as<node::Custom>());
        writeDocument(stream, schema->as<node::Custom>().getDefinition(), dumper);
    } else if (schema->is<node::Enum>()) {
        int index = readZigZagLong(stream);
        dumper.Enum(schema->as<node::Enum>(), index);

    } else if (schema->is<node::Array>()) {
        auto &a = schema->as<node::Array>();
        auto const &node = a.getItemsType();

        dumper.ArrayBegin(a);
        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                writeDocument(stream, node, dumper);
            }
        } while(objectsInBlock != 0);
        dumper.ArrayEnd(a);

    } else if (schema->is<node::Map>()) {
        auto &m = schema->as<node::Map>();
        auto const &node = m.getItemsType();

        assert(node->is<node::String>());
        dumper.MapBegin(m);
        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                const auto & name  = stream.getString(readZigZagLong(stream));
                const auto & value = stream.getString(readZigZagLong(stream));
                dumper.MapName(name, node->as<node::String>());
                dumper.MapValue(value, node->as<node::String>());
                // writeDocument(stream, node, dumper);
            }
        } while(objectsInBlock != 0);
        dumper.MapEnd(m);

    } else {
        if (schema->is<node::String>()) {
            dumper.String(stream.getString(readZigZagLong(stream)), schema->as<node::String>());
        } else if (schema->is<node::Int>()) {
            int value = readZigZagLong(stream);
            dumper.Int(value, schema->as<node::Int>());
            // std::cout << schema->getItemName() << ": " << value << std::endl;
        } else if (schema->is<node::Long>()) {
            long value = readZigZagLong(stream);
            dumper.Long(value, schema->as<node::Long>());
            // std::cout << schema->getItemName() << ": " << value << std::endl;
        } else if (schema->is<node::Float>()) {
            // readFloat(stream);
            // std::cout << schema->getItemName() << ": " << readFloat(stream) << std::endl;
            float value = readFloat(stream);
            dumper.Float(value, schema->as<node::Float>());
        } else if (schema->is<node::Double>()) {
            // std::cout << schema->getItemName() << ": " << readDouble(stream) << std::endl;
            double value = readDouble(stream);
            dumper.Double(value, schema->as<node::Double>());
        } else if (schema->is<node::Boolean>()) {
            // std::cout << schema->getItemName() << ": " << readBoolean(stream) << std::endl;
            bool value = readBoolean(stream);
            dumper.Boolean(value, schema->as<node::Boolean>());
        } else if (schema->is<node::Null>()) {
            // std::cout << schema->getItemName() << ": null" << std::endl;
            dumper.Null(schema->as<node::Null>());
        } else {
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }
    }
}


void Reader::dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, int level) {
    if (schema->is<node::Record>()) {
        std::cout << "{\n";
        for(auto &p : schema->as<node::Record>().getChildren()) {
            dumpDocument(stream, p, level + 1);
        }
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << "}\n";
    } else if (schema->is<node::Union>()) {
        int item = readZigZagLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        dumpDocument(stream, node, level);
    } else if (schema->is<node::Custom>()) {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << schema->getItemName() /* << ":" << schema->getTypeName() << std::endl */ ;
        dumpDocument(stream, schema->as<node::Custom>().getDefinition(), level);
    } else if (schema->is<node::Enum>()) {
        int index = readZigZagLong(stream);
        const std::string &value = schema->as<node::Enum>()[index];
        std::cout << /* schema->getItemName() <<*/ ": \"" << value  << '"' << std::endl;
    } else if (schema->is<node::Array>()) {
        auto const &node = schema->as<node::Array>().getItemsType();

        std::cout << "[\n";
        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                dumpDocument(stream, node, level + 1);
            }
        } while(objectsInBlock != 0);
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << "]\n";
    } else if (schema->is<node::Map>()) {
        auto const &node = schema->as<node::Map>().getItemsType();

        std::cout << "[\n";
        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                for(int k = 0; k < level+1; ++k) {
                    std::cout << "\t";
                }
                std::cout << readString(stream);
                dumpDocument(stream, node, 1);
            }
        } while(objectsInBlock != 0);
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << "]\n";
    } else {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        if (schema->is<node::String>()) {
            const std::string value = stream.getStdString(readZigZagLong(stream));
            std::cout << schema->getItemName() << ": \"" << value  << '"' << std::endl;
        } else if (schema->is<node::Int>()) {
            int value = readZigZagLong(stream);
            std::cout << schema->getItemName() << ": " << value << std::endl;
        } else if (schema->is<node::Long>()) {
            int value = readZigZagLong(stream);
            std::cout << schema->getItemName() << ": " << value << std::endl;
        } else if (schema->is<node::Float>()) {
            // readFloat(stream);
            std::cout << schema->getItemName() << ": " << readFloat(stream) << std::endl;
        } else if (schema->is<node::Double>()) {
            std::cout << schema->getItemName() << ": " << readDouble(stream) << std::endl;
        } else if (schema->is<node::Boolean>()) {
            std::cout << schema->getItemName() << ": " << readBoolean(stream) << std::endl;
        } else if (schema->is<node::Null>()) {
            std::cout << schema->getItemName() << ": null" << std::endl;
        } else {
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }
    }
}

bool Reader::eof() {
    return d->input->eof();
}


void Reader::dumpShema(const std::unique_ptr<Node> &schema, int level) const {
    if (schema->is<node::Record>()) {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << schema->getItemName() << " {\n";
        for(auto &p : schema->as<node::Record>().getChildren()) {
            dumpShema(p, level + 1);
        }
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << "}\n";
    } else if (schema->is<node::Union>()) {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << schema->getItemName() << ":[\n";
        for(auto &p : schema->as<node::Union>().getChildren()) {
            dumpShema(p, level + 1);
        }
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << "]\n";
    } else if (schema->is<node::Custom>()) {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
        dumpShema(schema->as<node::Custom>().getDefinition(), level + 1);
    } else {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        // std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
    }
}

std::string Reader::readString(DeflatedBuffer &input) {
    int64_t len = readZigZagLong(input);
    std::string result;
    result.resize(len);
    input.read(&result[0], result.size());

    return result;
}

float Reader::readFloat(DeflatedBuffer &input) {
    static_assert(sizeof(float) == 4, "sizeof(float) should be == 4");

    union {
        uint8_t bytes[4];
        float result;
    } buffer;

    input.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

    return buffer.result;
}

double Reader::readDouble(DeflatedBuffer &input) {
    static_assert(sizeof(double) == 8, "sizeof(double) should be == 8");

    union {
        uint8_t bytes[8];
        double result;
    } buffer;

    input.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

    return buffer.result;
}

bool Reader::readBoolean(DeflatedBuffer &input) {
    uint8_t c = input.getChar();

    return c == 1;
}

std::map<int, int> Reader::compileFieldsList(const std::string &filedList, const header &header) {

    std::vector<std::string> fields;
    boost::algorithm::split(fields, filedList, boost::is_any_of(","));

    std::map<int, int> result;
    int pos = 0;
    for(auto p = fields.begin(); p != fields.end(); ++p) {
        const auto node = schemaNodeByPath(*p, header);
        result[node->getNumber()] = pos;
        pos++;
    }

    return result;

}

FilterExpression Reader::compileCondition(const std::string &what, const std::string &condition, const header &header) {

    FilterExpression result;

    auto currentNode = schemaNodeByPath(what, header);

    if (currentNode->is<node::String>()) {
        result.strValue = condition;
    } else if (currentNode->is<node::Int>()) {
        result.value.i = boost::lexical_cast<int>(condition);
    } else if (currentNode->is<node::Long>()) {
        result.value.i = boost::lexical_cast<int>(condition);
    } else if (currentNode->is<node::Union>()) {
        for( auto &p : currentNode->as<node::Union>().getChildren()) {
            if (p->is<node::String>()) {
                result.strValue = condition;
                currentNode = p.get();
                break;
            } else if (p->is<node::Int>()) {
                result.value.i = boost::lexical_cast<int>(condition);
                currentNode = p.get();
                break;
            }
        }
    } else {
        std::cout << "Unsupported type: " << currentNode->getTypeName() << " name=" <<currentNode->getItemName() << std::endl;

        throw PathNotFound();
    }

    result.shemaItem = currentNode;
    result.what = what;


    return result;
}

const Node *Reader::schemaNodeByPath(const std::string &path, const header &header) {
    std::vector<std::string> chunks;
    boost::algorithm::split(chunks, path, boost::is_any_of("."));

    auto currentNode = header.schema.get();

    for(auto p = chunks.begin(); p != chunks.end(); ++p) {
        //std::cout << "XX " << *p << std::endl;
        if (currentNode->is<node::Custom>()) {
            currentNode = currentNode->as<node::Custom>().getDefinition().get();
        }
        if (currentNode->is<node::Record>()) {
            for( auto &n : currentNode->as<node::Record>().getChildren()) {
                //std::cout << "checking " << n->getItemName() << std::endl;
                if (n->getItemName() == *p) {
                    //std::cout << " found " << *p << std::endl;
                    currentNode = n.get();
                    goto next;
                }
            }
            // std::cout << " not found " << *p << std::endl;
            throw PathNotFound();
        } else {
            std::cout << "Can't find path" << std::endl;
            throw PathNotFound();
        }

        throw PathNotFound();
next://
        (void)currentNode;
    }

    return currentNode;
}

}

