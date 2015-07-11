#pragma once

namespace filter {
struct equality_expression;
}

namespace avro {

class StringBuffer;

namespace predicate {

class Predicate {
public:

    Predicate(filter::equality_expression *expr);
    virtual ~Predicate();

    template <typename T>
    void apply(const T &sb);

    void setIsNull(bool isNull);
private:
    filter::equality_expression *expr;

    template <typename T>
    void applyNumeric(const T &sb);
};

}
}