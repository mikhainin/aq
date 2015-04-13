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
#include "node/boolean.h"
#include "node/enum.h"
#include "node/float.h"
#include "node/double.h"
#include "node/null.h"

#include "deflatedbuffer.h"
#include "stringbuffer.h"
#include "zigzag.hpp"
#include "reader.h"

namespace {
const std::string AVRO_MAGICK = "Obj\001"; // 4 bytes
}

namespace avro {

class Reader::Private {
public:
    std::string filename;
    const char *mmappedFile = nullptr;
    size_t fileLength = 0;

    int fd = -1;

    std::unique_ptr<StringBuffer> input;

    DeflatedBuffer deflate_buffer;
    Limiter &limit;


    Private(Limiter &limit) : limit(limit) {
    }
};

Reader::Reader(const std::string& filename, Limiter &limit) :
    d(new Private(limit)) {

    d->filename = filename;


    struct stat st;
    stat(d->filename.c_str(), &st);
    d->fileLength = st.st_size;

    d->fd = open(d->filename.c_str(), O_RDONLY);
    // TODO: check if opened
    assert(d->fd > 0);

    size_t len = (d->fileLength/4096 + 1) * 4096;

    d->mmappedFile =
     (const char *)mmap(nullptr, len, PROT_READ, MAP_PRIVATE, d->fd, 0);
    if (d->mmappedFile == MAP_FAILED) {
        // TODO: handle this case correctyl
        // close FD, provide useful information how to avoid this error
        perror(d->filename.c_str());
        throw std::runtime_error("Can't mmap file " + d->filename);
    }

    d->input.reset(new StringBuffer(d->mmappedFile, d->fileLength));


    // d->input.open(filename);
}

Reader::~Reader() {
    if (d->mmappedFile && d->mmappedFile != MAP_FAILED) {
        munmap((void*)d->mmappedFile, (d->fileLength/4096 + 1) * 4096);
        d->mmappedFile = nullptr;
    }
    if (d->fd) {
        close(d->fd);
        d->fd = 0;
    }
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
    // d->input.read(&c, 1);

    assert(c == 0); // TODO: what is it?
    d->input->read(&header.sync[0], sizeof header.sync);


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

    int64_t objectCountInBlock = readZigZagLong(*d->input);
    int64_t blockBytesNum = readZigZagLong(*d->input);

    if (header.metadata.at("avro.codec") == "deflate") { // TODO: check it once

        d->deflate_buffer.assignData(d->input->getAndSkip(blockBytesNum), blockBytesNum);

        for(int i = 0; i < objectCountInBlock; ++i) {
            // TODO: rewrite it using hierarcy of filters/decoders.
            // TODO: implement counter as a filter  
            if (!filter) {
                dumpDocument(d->deflate_buffer, header.schema, 0);
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

void Reader::dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, int level) {
    if (schema->is<node::Record>()) {

        /*for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        } */
        std::cout << "{\n";
        for(auto &p : schema->as<node::Record>().getChildren()) {
            dumpDocument(stream, p, level + 1);
        }
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << "}\n";
    } else if (schema->is<node::Union>()) {
        /*for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }*/
        int item = readZigZagLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        // std::cout << "union " << node->getItemName() << ": " << node->getTypeName() << std::endl;
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


}

