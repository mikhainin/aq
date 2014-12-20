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

namespace {
const std::string AVRO_MAGICK = "Obj\001"; // 4 bytes
}

#include "reader.h"

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

    d->input.read(&header.sync[0], sizeof header.sync);

    return header;

}


void Reader::readBlock(const header &header) {

    int64_t blockStart = d->input.tellg();

    int64_t objectCountInBlock = readLong();
    int64_t blockBytesNum = readLong();

    int64_t blockHeaderLen = d->input.tellg() - blockStart;

    std::cout << objectCountInBlock << std::endl;
    std::cout << blockBytesNum << std::endl;

    d->input.seekg(blockBytesNum - blockHeaderLen - 16, std::ios_base::cur); // TODO: move to a function

    char tmp_sync[16] = {0}; // TODO sync length to a constant
    d->input.read(&tmp_sync[0], sizeof tmp_sync); // TODO: move to a function

    if (std::memcmp(tmp_sync, header.sync, sizeof tmp_sync) != 0) {
        throw std::runtime_error("Sync match failed");
    }

}

bool Reader::eof() {
    return d->input;
}

// TODO: rewrite it
int64_t decodeZigzag64(uint64_t input);
int64_t Reader::readLong() {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;
    do {
        if (shift >= 64) {
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

}

