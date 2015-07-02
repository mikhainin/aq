
#include <filter/equality_expression.h>

#include <avro/stringbuffer.h>

#include "predicate.h"

namespace avro {
namespace predicate {


Predicate::Predicate(filter::equality_expression *expr) : expr(expr) {
}

Predicate::~Predicate() {
}

void Predicate::applyString(const StringBuffer &sb) {

    const std::string & str = boost::get<std::string>(expr->constant);

    if (expr->op == filter::equality_expression::EQ) {
        expr->setState(sb == str);
    } else if (expr->op == filter::equality_expression::NE) {
        expr->setState(sb != str);
        // std::cout << "state " << (sb != str) << std::endl;
    } else {
        assert(false && "expr->op contains unknown operator");
    }
}

void Predicate::applyInt(int i) {
    // TODO: use pattern "strategy" here
    if (expr->op == filter::equality_expression::EQ) {
        expr->setState(boost::get<int>(expr->constant) == i);
    } else if (expr->op == filter::equality_expression::NE) {
        expr->setState(boost::get<int>(expr->constant) != i);
    } else {
        assert(false && "expr->op contains unknown operator");
    }
}

}
}