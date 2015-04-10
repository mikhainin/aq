//
//  zigzag.hpp
//  avroq
//
//  Created by Mikhail Galanin on 08/04/15.
//  Copyright (c) 2015 Mikhail Galanin. All rights reserved.
//

#ifndef avroq_zigzag_hpp
#define avroq_zigzag_hpp

#include "eof.h"

namespace avro {

inline
int64_t decodeZigzag64(uint64_t input)
{
    return static_cast<int64_t>(((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

template <class BufferType>
inline
long readZigZagLong(BufferType &b) {
    uint64_t encoded = 0;
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
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;

        // d->read.push_back(u);


    } while (u & 0x80);
    
    return decodeZigzag64(encoded);

}

}
#endif
