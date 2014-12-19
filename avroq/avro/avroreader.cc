//
//  avroreader.cc
//  avroq
//
//  Created by Mikhail Galanin on 12/12/14.
//  Copyright (c) 2014 Mikhail Galanin. All rights reserved.
//

#include <cstdint>
#include <fstream>

#include <iostream>

const std::string AVRO_MAGICK = "Obj\001"; // 4 bytes

#include "avroreader.h"

void AvroReader::readFile(const std::string & filename) {
    std::ifstream input(filename);
    
    std::string magick;
    magick.resize(AVRO_MAGICK.size());
    input.read(&magick[0], AVRO_MAGICK.size());
    
    if (magick != AVRO_MAGICK) {
        throw std::runtime_error("Bad avro file");
    }
    
    int64_t recordsNumber = readLong(input);
    
    std::cout << recordsNumber << std::endl;
    // std::cout << readLong(input) << std::endl;
    std::cout << readString(input) << std::endl;
    std::cout << readString(input) << std::endl;

}

// TODO: rewrite it
int64_t decodeZigzag64(uint64_t input);
int64_t AvroReader::readLong(std::ifstream &input) {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;
    do {
        if (shift >= 64) {
            throw std::runtime_error("Invalid Avro varint");
        }
        u = static_cast<uint8_t>(input.get());
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

std::string AvroReader::readString(std::ifstream &input) {
    int64_t len = readLong(input);
    std::string result;
    result.resize(len);
    input.read(&result[0], result.size());
    
    return result;
}

