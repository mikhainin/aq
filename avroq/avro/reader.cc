//
//  Reader.cc
//  avroq
//
//  Created by Mikhail Galanin on 12/12/14.
//  Copyright (c) 2014 Mikhail Galanin. All rights reserved.
//

#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <iostream>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include "schemareader.h"
#include "schemanode.h"
#include "reader.h"

namespace {
const std::string AVRO_MAGICK = "Obj\001"; // 4 bytes
}

namespace avro {

class Reader::Private {
public:
    std::string filename;
    std::ifstream input;
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


    int64_t recordsNumber = readLong();
    
    for(uint i = 0; i < recordsNumber; ++i) {

        const std::string &key = readString();
        const std::string &value = readString();

        header.metadata[key] = value;

    }

    SchemaReader schemaReader(header.metadata["avro.schema"]);
    schemaReader.parse();

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

void Reader::readBlock(const header &header) {

    throw Eof();

    int64_t objectCountInBlock = readLong();
    int64_t blockBytesNum = readLong();


    std::cout << "block objects count: " << objectCountInBlock << std::endl;
    std::cout << "block length: " << blockBytesNum << std::endl;

    if (header.metadata.at("avro.codec") == "deflate") { // TODO: check it once

        std::unique_ptr<boost::iostreams::filtering_istream> deflate_stream(new boost::iostreams::filtering_istream());

        deflate_stream->push(boost::iostreams::zlib_decompressor(get_zlib_params()));

        deflate_stream->push(my_input_filter(d->input, blockBytesNum));

        int i = 0;
        while(*deflate_stream) {
            i++;
            char c;
            deflate_stream->read(&c, 1);
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
    std::cout << std::endl;

}

bool Reader::eof() {
    return d->input.eof();
}

// TODO: rewrite it
int64_t decodeZigzag64(uint64_t input);
int64_t Reader::readLong() {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;
    do {
        if (shift >= 64) {
            if (d->input.eof()) {
                throw Eof();
            }
            throw std::runtime_error("Invalid Avro varint");
        }
        u = static_cast<uint8_t>(d->input.get());
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;
    } while (u & 0x80);
    
    return decodeZigzag64(encoded);
}

int64_t
decodeZigzag64(uint64_t input)
{
    return static_cast<int64_t>(((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

std::string Reader::readString() {
    int64_t len = readLong();
    std::string result;
    result.resize(len);
    d->input.read(&result[0], result.size());
    
    return result;
}


boost::iostreams::zlib_params get_zlib_params() {
  boost::iostreams::zlib_params result;
  result.method = boost::iostreams::zlib::deflated;
  result.noheader = true;
  return result;
}


}

