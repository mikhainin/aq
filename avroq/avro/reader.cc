#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <unordered_map>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include <boost/algorithm/string.hpp>

#include <filter/filter.h>
#include <filter/equality_expression.h>


#include "dumper/tsv.h"
#include "dumper/fool.h"

#include "node/all_nodes.h"
#include "node/nodebypath.h"

#include "block.h"
#include "deflatedbuffer.h"
#include "filehandler.h"
#include "limiter.h"
#include "predicate/predicate.h"
#include "reader.h"
#include "schemareader.h"
#include "stringbuffer.h"
#include "zigzag.hpp"

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
    std::shared_ptr<filter::Filter> filter;
    std::unordered_multimap<const node::Node *, std::shared_ptr<predicate::Predicate>> filterItems;

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


struct ToDouble {
    using result_type = double;
    double operator() (double d) const {
        return d;
    }
    double operator() (float f) const {
        return f;
    }
    double operator() (int i) const {
        return i;
    }

    template <typename T>
    double operator() (const T&) const {
        throw std::runtime_error("wrong type");
    }
};


struct ToFloat {
    using result_type = float;
    float operator() (double d) const {
        return d;
    }
    float operator() (float f) const {
        return f;
    }
    float operator() (int i) const {
        return i;
    }

    template <typename T>
    float operator() (const T&) const {
        throw std::runtime_error("wrong type");
    }
};

struct ToInt {
    using result_type = int;
    int operator() (double d) const {
        return d;
    }
    int operator() (float f) const {
        return f;
    }
    int operator() (int i) const {
        return i;
    }

    template <typename T>
    float operator() (const T&) const {
        throw std::runtime_error("wrong type");
    }
};

void Reader::setFilter(std::shared_ptr<filter::Filter> filter, const header &header) {
    d->filter = filter;

    for(auto &filterPredicate : filter->getPredicates()) {
        const node::Node * filterNode = schemaNodeByPath(filterPredicate->identifier, header);

        if (filterNode->is<node::Union>()) {
            for( auto &p : filterNode->as<node::Union>().getChildren()) {
                filterNode = p.get();
                if (filterNode->is<node::Custom>()) {
                    filterNode = filterNode->as<node::Custom>().getDefinition().get();
                }
                if (filterNode->isOneOf<
                            node::Boolean,
                            node::Double,
                            node::Enum,
                            node::Float,
                            node::Int,
                            node::Long,
                            node::String>()) {
                    break;
                }
            }
        }
        if (filterNode->isOneOf<node::String, node::Boolean>()) {
            ; // ok
        } else if (filterNode->is<node::Enum>()) {
            auto const &e = filterNode->as<node::Enum>();
            int i = e.findIndexForValue(boost::get<std::string>(filterPredicate->constant));
            if (i == -1) {
                // TODO: add list of valid values
                throw std::runtime_error("Invalid value for enum field '" + filterPredicate->identifier + "'");
            }
            filterPredicate->constant = i;
        } else if (filterNode->is<node::Double>()) {
            filterPredicate->constant = boost::apply_visitor(ToDouble(), filterPredicate->constant);
        } else if (filterNode->is<node::Float>()) {
            filterPredicate->constant = boost::apply_visitor(ToFloat(), filterPredicate->constant);
        } else if (filterNode->isOneOf<node::Int, node::Long>()) {
            filterPredicate->constant = boost::apply_visitor(ToInt(), filterPredicate->constant);
        } else {
            throw std::runtime_error(
                "Sorry, but type '" + filterNode->getTypeName() +
                "' for field '" + filterPredicate->identifier + "' "
                "Is not yet supported in filter expression.");
        }
        d->filterItems.insert(
                std::make_pair(
                    filterNode,
                    std::make_shared<predicate::Predicate>(filterPredicate)
                )
            );
    }
}

std::unique_ptr<Block> Reader::nextBlock(const header &header) {

    int64_t objectCountInBlock = readZigZagLong(*d->input);
    int64_t blockBytesNum = readZigZagLong(*d->input);
    std::unique_ptr<Block> block(new Block);

    block->buffer.assignData(d->input->getAndSkip(blockBytesNum), blockBytesNum);
    block->objectCount = objectCountInBlock;

    return block;

}


void Reader::readBlock(const header &header, const dumper::TsvExpression &wd) {

    int64_t objectCountInBlock = readZigZagLong(*d->input);
    int64_t blockBytesNum = readZigZagLong(*d->input);

    if (header.metadata.at("avro.codec") == "deflate") { // TODO: check it once

        d->deflate_buffer.assignData(d->input->getAndSkip(blockBytesNum), blockBytesNum);

        for(int i = 0; i < objectCountInBlock; ++i) {
            // TODO: rewrite it using hierarcy of filters/decoders.
            // TODO: implement counter as a filter  
            d->deflate_buffer.startDocument();
            decodeDocument(d->deflate_buffer, header.schema);
            if (!d->filter || d->filter->expressionPassed()) {
                d->deflate_buffer.resetToDocument();
                if (wd.pos > 0) {
                    dumper::Tsv dumper(wd);
                    dumpDocument(d->deflate_buffer, header.schema, dumper);
                    dumper.EndDocument();
                } else {
                    auto dumper = dumper::Fool();
                    dumpDocument(d->deflate_buffer, header.schema, dumper);
                    dumper.EndDocument();
                }
                d->limit.documentFinished();
                if(d->filter) {
                    d->filter->resetState();
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

void Reader::decodeDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema) {
    if (schema->is<node::Record>()) {
        for(auto &p : schema->as<node::Record>().getChildren()) {
            decodeDocument(stream, p);
        }
    } else if (schema->is<node::Union>()) {
        int item = readZigZagLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        decodeDocument(stream, node);
    } else if (schema->is<node::Custom>()) {
        decodeDocument(stream, schema->as<node::Custom>().getDefinition());
    } else if (schema->is<node::Enum>()) {
        skipOrApplyFilter<int>(stream, schema);
    } else if (schema->is<node::Array>()) {
        auto const &node = schema->as<node::Array>().getItemsType();

        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                decodeDocument(stream, node);
            }
        } while(objectsInBlock != 0);

    } else if (schema->is<node::Map>()) {
        auto const &node = schema->as<node::Map>().getItemsType();

        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                readString(stream);
                decodeDocument(stream, node);
            }
        } while(objectsInBlock != 0);
    } else {
        if (schema->is<node::String>()) {
            skipOrApplyFilter<StringBuffer>(stream, schema);
        } else if (schema->is<node::Int>()) {
            skipOrApplyFilter<int>(stream, schema);
        } else if (schema->is<node::Long>()) {
            skipOrApplyFilter<long>(stream, schema);
        } else if (schema->is<node::Float>()) {
            skipOrApplyFilter<float>(stream, schema);
        } else if (schema->is<node::Double>()) {
            skipOrApplyFilter<double>(stream, schema);
        } else if (schema->is<node::Boolean>()) {
            skipOrApplyFilter<bool>(stream, schema);
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
void Reader::dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema, T &dumper) {
    if (schema->is<node::Record>()) {

        auto &r = schema->as<node::Record>();
        dumper.RecordBegin(r);
        for(auto &p : schema->as<node::Record>().getChildren()) {
            dumpDocument(stream, p, dumper);
        }
        dumper.RecordEnd(r);

    } else if (schema->is<node::Union>()) {
        int item = readZigZagLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        dumpDocument(stream, node, dumper);
    } else if (schema->is<node::Custom>()) {
        dumper.CustomBegin(schema->as<node::Custom>());
        dumpDocument(stream, schema->as<node::Custom>().getDefinition(), dumper);
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
                dumpDocument(stream, node, dumper);
            }
        } while(objectsInBlock != 0);
        dumper.ArrayEnd(a);

    } else if (schema->is<node::Map>()) {
        auto &m = schema->as<node::Map>();
        auto const &node = m.getItemsType();

        // TODO: refactor this trash
        assert(node->is<node::String>() || node->is<node::Int>());
        dumper.MapBegin(m);
        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                const auto & name  = stream.getString(readZigZagLong(stream));
                dumper.MapName(name);

                // TODO: refactor this trash
                if (node->is<node::String>()) {
                    const auto & value = stream.getString(readZigZagLong(stream));
                    dumper.MapValue(value, node->as<node::String>());
                } else if (node->is<node::Int>()) {
                     const auto & value = readZigZagLong(stream);
                    dumper.MapValue(value, node->as<node::Int>());
               }
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
            float value = read<float>(stream);
            dumper.Float(value, schema->as<node::Float>());
        } else if (schema->is<node::Double>()) {
            // std::cout << schema->getItemName() << ": " << readDouble(stream) << std::endl;
            double value = read<double>(stream);
            dumper.Double(value, schema->as<node::Double>());
        } else if (schema->is<node::Boolean>()) {
            // std::cout << schema->getItemName() << ": " << readBoolean(stream) << std::endl;
            bool value = read<bool>(stream);
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

template <typename T>
void Reader::skipOrApplyFilter(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema) {
    if (d->filter) {
        auto range = d->filterItems.equal_range(schema.get());
        if (range.first != range.second) {
            const auto &value = read<T>(stream);
            for_each (
                range.first,
                range.second,
                [&value](const auto& filterItem){
                    filterItem.second->template apply<T>(value);
                }
            );
            return;
        }
    }
    skip<T>(stream);
}

template <>
StringBuffer Reader::read(DeflatedBuffer &input) {
    int64_t len = readZigZagLong(input);
    return input.getString(len);
}

template <>
void Reader::skip<StringBuffer>(DeflatedBuffer &input) {
    int64_t len = readZigZagLong(input);
    input.skip(len);
}


template <>
long Reader::read(DeflatedBuffer &input) {
    return readZigZagLong(input);
}

template <>
void Reader::skip<long>(DeflatedBuffer &input) {
    skipInt(input);
}



template <>
int Reader::read(DeflatedBuffer &input) {
    return readZigZagLong(input);
}

template <>
void Reader::skip<int>(DeflatedBuffer &input) {
    skipInt(input);
}


template <>
bool Reader::read(DeflatedBuffer &input) {
    uint8_t c = input.getChar();
    return c == 1;
}

template <>
void Reader::skip<bool>(DeflatedBuffer &input) {
    input.skip(1);
}



template <>
float Reader::read(DeflatedBuffer &input) {
    static_assert(sizeof(float) == 4, "sizeof(float) should be == 4");

    union {
        uint8_t bytes[4];
        float result;
    } buffer;

    input.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

    return buffer.result;
}

template <>
void Reader::skip<float>(DeflatedBuffer &input) {
    static_assert(sizeof(float) == 4, "sizeof(float) should be == 4");
    input.skip(4);
}


template <>
double Reader::read(DeflatedBuffer &input) {
    static_assert(sizeof(double) == 8, "sizeof(double) should be == 8");

    union {
        uint8_t bytes[8];
        double result;
    } buffer;

    input.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

    return buffer.result;
}

template <>
void Reader::skip<double>(DeflatedBuffer &input) {
    static_assert(sizeof(double) == 8, "sizeof(double) should be == 8");
    input.skip(8);
}



bool Reader::eof() {
    return d->input->eof();
}


void Reader::dumpShema(const std::unique_ptr<node::Node> &schema, int level) const {
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

StringBuffer Reader::readStringBuffer(DeflatedBuffer &input) {
    int64_t len = readZigZagLong(input);
    return input.getString(len);
}


void Reader::skipString(DeflatedBuffer &input) {
    int64_t len = readZigZagLong(input);
    input.skip(len);
}

void Reader::skipInt(DeflatedBuffer &b) {
    int shift = 0;
    uint8_t u;
    do {
        if (shift >= 64) {
            if (b.eof()) {
                throw Eof();
            }
            throw std::runtime_error("Invalid Avro varint");
        }
        u = static_cast<uint8_t>(b.getChar());
        shift += 7;

        // d->read.push_back(u);


    } while (u & 0x80);
}

dumper::TsvExpression Reader::compileFieldsList(const std::string &filedList, const header &header) {

    std::vector<std::string> fields;
    boost::algorithm::split(fields, filedList, boost::is_any_of(","));

    dumper::TsvExpression result;
    result.pos = 0;

    if (filedList.empty()) {
    	return result;
    }

    for(auto p = fields.begin(); p != fields.end(); ++p) {
        const auto node = schemaNodeByPath(*p, header);
        result.what[node->getNumber()] = result.pos;
        if (node->is<node::Union>()) {
        	for( auto &p : node->as<node::Union>().getChildren()) {
        		result.what[p->getNumber()] = result.pos;
        	}
        }
        result.pos++;
    }

    return result;

}

const node::Node* Reader::schemaNodeByPath(const std::string &path, const header &header) {
    auto n = node::nodeByPath(path, header.schema.get());
    if (n == nullptr) {
        throw PathNotFound(path);
    }
    return n;
}

}

