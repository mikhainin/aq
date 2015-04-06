
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
#include "node/node.h"
#include "node/record.h"
#include "node/union.h"
#include "node/string.h"
#include "node/custom.h"
#include "node/int.h"
#include "node/boolean.h"
#include "node/float.h"
#include "node/double.h"
#include "node/null.h"

#include "reader.h"

namespace {
const std::string AVRO_MAGICK = "Obj\001"; // 4 bytes
}

namespace avro {

class Reader::Private {
public:
    std::string filename;
    std::ifstream input;
    // std::vector<uint8_t> read; // for debug purposes
};

Reader::Reader(const std::string& filename) :
    d(new Private()) {

    d->filename = filename;
    d->input.open(filename);
}

Reader::~Reader() {
    delete d;
}


header Reader::readHeader() {

    
    std::string magick;
    magick.resize(AVRO_MAGICK.size());
    d->input.read(&magick[0], AVRO_MAGICK.size());
    
    if (magick != AVRO_MAGICK) {
        throw std::runtime_error("Bad avro file (wrong magick)");
    }

    header header;


    int64_t recordsNumber = readLong(d->input);
    
    for(uint i = 0; i < recordsNumber; ++i) {

        const std::string &key = readString(d->input);
        const std::string &value = readString(d->input);

        header.metadata[key] = value;

    }

    SchemaReader schemaReader(header.metadata["avro.schema"]);
    header.schema = schemaReader.parse();
    header.nodesNumber = schemaReader.nodesNumber();
    //dumpShema(schemaRoot);
    
    char c;
    d->input.read(&c, 1);

    assert(c == 0); // TODO: what is it?
    d->input.read(&header.sync[0], sizeof header.sync);

    return header;

}

boost::iostreams::zlib_params get_zlib_params();

class my_input_filter : public boost::iostreams::multichar_input_filter {
public:
    template<typename Source>
    std::streamsize read(Source &, char* s, std::streamsize n)
    {
        if (bytesLeft == 0) {
            return -1;
        }

        std::streamsize bytesRead = -1; // EOF;

        if (bytesLeft >= n) {

            bytesRead = boost::iostreams::read(source, s, n);

        } else if (bytesLeft < n) {
            bytesRead = boost::iostreams::read(source, s, bytesLeft);
        }

        if (bytesRead > 0) {
            bytesLeft -= bytesRead;
        }

        return bytesRead;
    }

    my_input_filter(std::ifstream &source, std::streamsize bytesLeft) :
        boost::iostreams::multichar_input_filter(), source(source), bytesLeft(bytesLeft) {
    }

private:
    std::ifstream &source;
    std::streamsize bytesLeft = 0;
};

void Reader::readBlock(const header &header, const FilterExpression *filter) {

    // throw Eof();

    int64_t objectCountInBlock = readLong(d->input);
    int64_t blockBytesNum = readLong(d->input);


    // std::cout << "block objects count: " << objectCountInBlock << std::endl;
    // std::cout << "block length: " << blockBytesNum << std::endl;

    if (header.metadata.at("avro.codec") == "deflate") { // TODO: check it once

        std::unique_ptr<boost::iostreams::filtering_istream> deflate_stream(new boost::iostreams::filtering_istream());

        deflate_stream->push(boost::iostreams::zlib_decompressor(get_zlib_params()));

        deflate_stream->push(my_input_filter(d->input, blockBytesNum));

        // int i = 0;
        /*while(*deflate_stream) {
            i++;
            char c;
            deflate_stream->read(&c, 1);
        }
        */
        for(int i = 0; i < objectCountInBlock; ++i) {
            decodeBlock(*deflate_stream, header.schema, filter);
        }

        deflate_stream->seekg(0, std::ios_base::end);
        // std::cout << "compressed size: " << i /*<< " buf size: " << compressed_.size()*/ << std::endl;

        char tmp_sync[16] = {0}; // TODO sync length to a constant
        d->input.read(&tmp_sync[0], sizeof tmp_sync); // TODO: move to a function

        if (std::memcmp(tmp_sync, header.sync, sizeof tmp_sync) != 0) {
            throw std::runtime_error("Sync match failed");
        }
        
    } else {
        throw std::runtime_error("Unknown codec: " + header.metadata.at("avro.codec"));
    }
    // std::cout << std::endl;

}

void skipLong(std::istream &input) {
    uint8_t u;
    do {
        if (input.eof()) {
            throw Reader::Eof();
        }

        u = static_cast<uint8_t>(input.get());
        // d->read.push_back(u);
    } while (u & 0x80);
}

void Reader::decodeBlock(boost::iostreams::filtering_istream &stream, const std::unique_ptr<Node> &schema, const FilterExpression *filter, int level) {
    if (schema->is<node::Record>()) {
        /*
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << schema->getItemName() << " {\n";*/
        for(auto &p : schema->as<node::Record>().getChildren()) {
            decodeBlock(stream, p, filter, level + 1);
        }
        /*for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << "}\n";*/
    } else if (schema->is<node::Union>()) {
        /*for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }*/
        int item = readLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        // std::cout << "union " << node->getItemName() << ": " << node->getTypeName() << std::endl;
        decodeBlock(stream, node, filter, level + 1);
    } else if (schema->is<node::Custom>()) {
        /*for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;*/
        decodeBlock(stream, schema->as<node::Custom>().getDefinition(), filter, level + 1);
    } else {
        /*for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }*/
        if (schema->is<node::String>()) {
            //
            const std::string value = readString(stream);
            if (schema.get() == filter->shemaItem && value == filter->strValue) {
                // dump ducument
            }
            // std::cout << schema->getItemName() << ": \"" << readString(stream)  << '"' << std::endl;
        } else if (schema->is<node::Int>()) {
            // skipLong(stream); //
            int value = readLong(stream);

            if (schema.get() == filter->shemaItem && value == filter->value.i) {
                // dump ducument
            }
            // std::cout << schema->getItemName() << ": " << readLong(stream) << std::endl;
        } else if (schema->is<node::Float>()) {
            readFloat(stream);
            // std::cout << schema->getItemName() << ": " << readFloat(stream) << std::endl;
        } else if (schema->is<node::Double>()) {
            readDouble(stream);
            // std::cout << schema->getItemName() << ": " << readDouble(stream) << std::endl;
        } else if (schema->is<node::Boolean>()) {
            readBoolean(stream);
            // std::cout << schema->getItemName() << ": " << readBoolean(stream) << std::endl;
        } else if (schema->is<node::Null>()) {
            // std::cout << schema->getItemName() << ": null" << std::endl;
        } else {
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }
        /*static int counter = 0;
        if (counter++ == 70) {
            throw Eof();
        }*/
    }
}

bool Reader::eof() {
    return d->input.eof();
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

// TODO: rewrite it
int64_t decodeZigzag64(uint64_t input);
int64_t Reader::readLong(std::istream &input) {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;
    do {
        if (shift >= 64) {
            if (input.eof()) {
                throw Eof();
            }
            throw std::runtime_error("Invalid Avro varint");
        }
        u = static_cast<uint8_t>(input.get());
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;

        // d->read.push_back(u);


    } while (u & 0x80);
    
    return decodeZigzag64(encoded);
}

inline
int64_t decodeZigzag64(uint64_t input)
{
    return static_cast<int64_t>(((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

std::string Reader::readString(std::istream &input) {
    int64_t len = readLong(input);
    std::string result;
    result.resize(len);
    input.read(&result[0], result.size());

    /*for(char c : result) {
        d->read.push_back(static_cast<uint8_t>(c));
    }*/
    return result;
}

float Reader::readFloat(std::istream &input) {
    static_assert(sizeof(float) == 4, "sizeof(float) should be == 4");

    union {
        uint8_t bytes[4];
        float result;
    } buffer;

    input.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

    return buffer.result;
}

double Reader::readDouble(std::istream &input) {
    static_assert(sizeof(double) == 8, "sizeof(double) should be == 8");

    union {
        uint8_t bytes[8];
        double result;
    } buffer;

    input.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

    return buffer.result;
}

bool Reader::readBoolean(std::istream &input) {
    uint8_t c;
    input.read(reinterpret_cast<char *>(&c), sizeof c);

    return c == 1;
}

boost::iostreams::zlib_params get_zlib_params() {
  boost::iostreams::zlib_params result;
  result.method = boost::iostreams::zlib::deflated;
  result.noheader = true;
  return result;
}

FilterExpression Reader::compileCondition(const std::string &what, const std::string &condition, const header &header) {

    FilterExpression result;
    std::vector<std::string> chunks;
    boost::algorithm::split(chunks, what, boost::is_any_of("."));

    auto currentNode = header.schema.get();

    for(auto p = chunks.begin(); p != chunks.end(); ++p) {
        std::cout << "XX " << *p << std::endl;
        if (currentNode->is<node::Custom>()) {
            currentNode = currentNode->as<node::Custom>().getDefinition().get();
        }
        if (currentNode->is<node::Record>()) {
            for( auto &n : currentNode->as<node::Record>().getChildren()) {
                std::cout << "checking " << n->getItemName() << std::endl;
                if (n->getItemName() == *p) {
                    std::cout << " found " << *p << std::endl;
                    currentNode = n.get();
                    goto next;
                }
            }
            std::cout << " not found " << *p << std::endl;
            throw PathNotFound();
        } else {
            std::cout << "Can't find path" << std::endl;
        }

        throw PathNotFound();
next://
        (void)currentNode;
    }

    result.shemaItem = currentNode;
    result.what = what;

    if (currentNode->is<node::String>()) {
        result.strValue = condition;
    } else if (currentNode->is<node::Int>()) {
        result.value.i = boost::lexical_cast<int>(condition);
    } else {
        std::cout << "Unsupported type: " << currentNode->getTypeName() << " name=" <<currentNode->getItemName() << std::endl;

        throw PathNotFound();
    }

    return result;
}


}

