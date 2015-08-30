#ifndef __avroq__avro_typeparser__
#define __avroq__avro_typeparser__

#include "zigzag.hpp"
#include "stringbuffer.h"

namespace avro {


template <class T>
class TypeParser {
};


template<>
class TypeParser<StringBuffer> {
public:

    template <class Stream>
    static StringBuffer read(Stream &stream) {
        int64_t len = readZigZagLong(stream);
        return stream.getString(len);
    }

    template <class Stream>
    static void skip(Stream &stream) {
        int64_t len = readZigZagLong(stream);
        stream.skip(len);
    }

};



template<>
class TypeParser<long> {
public:

    template <class Stream>
    static long read(Stream &stream) {
        return readZigZagLong(stream);
    }

    template <class Stream>
    static void skip(Stream &stream) {
        skipZigZagLong(stream);
    }
};



template<>
class TypeParser<int> {
public:

    template <class Stream>
    static int read(Stream &stream) {
        return readZigZagLong(stream);
    }

    template <class Stream>
    static void skip(Stream &stream) {
        skipZigZagLong(stream);
    }

};

template<>
class TypeParser<bool> {
public:

    template <class Stream>
    static bool read(Stream &stream) {
        uint8_t c = stream.getChar();
        return c == 1;
    }

    template <class Stream>
    static void skip(Stream &stream) {
        stream.skip(1);
    }
};



template<>
class TypeParser<float> {
public:

    template <class Stream>
    static float read(Stream &stream) {
        static_assert(sizeof(float) == 4, "sizeof(float) should be == 4");

        union {
            uint8_t bytes[4];
            float result;
        } buffer;

        stream.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

        return buffer.result;
    }

    template <class Stream>
    static void skip(Stream &stream) {
        static_assert(sizeof(float) == 4, "sizeof(float) should be == 4");
        stream.skip(4);
    }
};


template<>
class TypeParser<double> {
public:

    template <class Stream>
    static double read(Stream &stream) {
        static_assert(sizeof(double) == 8, "sizeof(double) should be == 8");

        union {
            uint8_t bytes[8];
            double result;
        } buffer;

        stream.read(reinterpret_cast<char *>(&buffer.bytes[0]), sizeof buffer.bytes);

        return buffer.result;
    }

    template <class Stream>
    static void skip(Stream &stream) {
        static_assert(sizeof(double) == 8, "sizeof(double) should be == 8");
        stream.skip(8);
    }

};


}

#endif