#pragma once

namespace filter {
struct equality_expression;
struct record_expression;
}

namespace avro {

class StringBuffer;

namespace predicate {

class Predicate {
    Predicate(const Predicate&) = delete;
protected:
    Predicate() {}
public:

    explicit Predicate(filter::equality_expression *expr);
    virtual ~Predicate();

    template <typename T>
    void apply(const T &sb);

    void setIsNull(bool isNull);

    virtual void pushArrayState();
    virtual void recordEnd();
private:
    filter::equality_expression *expr;

    template <typename T>
    void applyNumeric(const T &sb);
};

class RecordPredicate : public Predicate {
public:
    explicit RecordPredicate(filter::record_expression *expr);

    virtual void pushArrayState();
    virtual void recordEnd();

    filter::record_expression *state;
};

}
}