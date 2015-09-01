
#include <stdexcept>
#include <iostream>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "node/all_nodes.h"
#include "node/nodebypath.h"

#include "exception.h"
#include "filehandler.h"
#include "schemareader.h"
#include "stringbuffer.h"
#include "zigzag.hpp"

#include "reader.h"

namespace {
    const std::string AVRO_MAGICK = "Obj\001"; // 4 bytes
    const size_t SYNC_LENGTH = 16;
}

namespace avro {

class Reader::Private {
public:

    FileHandle file;

    std::unique_ptr<StringBuffer> input;

    Private(const std::string& filename)
        : file(filename),
          input(file.mmapFile()) {
    }
};

Reader::Reader(const std::string& filename) :
    d(new Private(filename)) {
}

Reader::~Reader() {
    delete d;
}


header Reader::readHeader() {

    
    std::string magick;
    magick.resize(AVRO_MAGICK.size());
    d->input->read(&magick[0], AVRO_MAGICK.size());
    
    if (magick != AVRO_MAGICK) {
        throw std::runtime_error("Bad avro file (wrong magick) on file " + d->file.fileName());
    }

    header header;

    int64_t recordsNumber = readZigZagLong(*d->input);
    
    for(uint i = 0; i < recordsNumber; ++i) {

        size_t len = readZigZagLong(*d->input);
        const std::string &key = d->input->getStdString(len);
        const std::string &value = d->input->getStdString(readZigZagLong(*d->input));

        header.metadata[key] = value;

    }

    char c = d->input->getChar();

    assert(c == 0); // Avro Map is over, no more elements
    d->input->read(&header.sync[0], sizeof header.sync);


    return header;

}

void Reader::parseSchema(header &header) {
    SchemaReader schemaReader(header.metadata["avro.schema"]);
    header.schema = schemaReader.parse();
    header.nodesNumber = schemaReader.nodesNumber();
    // dumpSchema(header.schema);
}

avro::StringBuffer Reader::nextBlock(const header &header, int64_t &objectCountInBlock ) {

    objectCountInBlock = readZigZagLong(*d->input);
    int64_t blockBytesNum = readZigZagLong(*d->input);

    if (blockBytesNum + SYNC_LENGTH > d->input->bytesLeft()) {
        throw std::runtime_error("Corrupted file (" + d->file.fileName() + "): next block supposed to have " +
            boost::lexical_cast<std::string>(blockBytesNum) + " bytes, but left: " +
            boost::lexical_cast<std::string>(d->input->bytesLeft()));
    }

    avro::StringBuffer result(d->input->getAndSkip(blockBytesNum), blockBytesNum);

    char tmp_sync[SYNC_LENGTH] = {0};
    d->input->read(&tmp_sync[0], sizeof tmp_sync); // TODO: move to a function

    if (std::memcmp(tmp_sync, header.sync, sizeof tmp_sync) != 0) {
        throw std::runtime_error("Sync match failed on file" + d->file.fileName());
    }

    return result;

}

bool Reader::eof() {
    return d->input->eof();
}


void Reader::dumpSchema(const std::unique_ptr<node::Node> &schema, int level) const {
    if (schema->is<node::Record>()) {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        std::cout << schema->getItemName() << " {\n";
        for(auto &p : schema->as<node::Record>().getChildren()) {
            dumpSchema(p, level + 1);
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
            dumpSchema(p, level + 1);
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
        dumpSchema(schema->as<node::Custom>().getDefinition(), level + 1);
    } else {
        for(int i = 0; i < level; ++i) {
            std::cout << "\t";
        }
        // std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
    }
}

const node::Node *notCustom(const node::Node *node) {

    if (node->is<node::Custom>()) {
        auto &p = node->as<node::Custom>().getDefinition();
        return notCustom(p.get());
    }
    return node;
}

dumper::TsvExpression Reader::compileFieldsList(const std::string &filedList, const header &header, const std::string &fieldSeparator) {

    std::vector<std::string> fields;
    boost::algorithm::split(fields, filedList, boost::is_any_of(","));

    dumper::TsvExpression result;
    result.pos = 0;
    result.fieldSeparator = fieldSeparator;

    if (filedList.empty()) {
    	return result;
    }

    for(auto p = fields.begin(); p != fields.end(); ++p) {

        // allow to insert spaces in TSV expression, ala " uuid, request.ip , request.uri "
        boost::algorithm::trim(*p);

        auto node = notCustom(schemaNodeByPath(*p, header));

        result.what.insert(
                std::make_pair(node->getNumber(), result.pos));

        if (node->is<node::Union>()) {
            for( auto &n : node->as<node::Union>().getChildren()) {
                result.what.insert(
                    std::make_pair(notCustom(n.get())->getNumber(), result.pos));
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

