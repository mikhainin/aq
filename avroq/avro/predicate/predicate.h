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

    virtual void applyString(const StringBuffer &sb);
    virtual void applyInt(int i);

private:
    filter::equality_expression *expr;
};

}
}