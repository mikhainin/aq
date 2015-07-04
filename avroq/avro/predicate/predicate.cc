
#include <filter/equality_expression.h>

#include <avro/stringbuffer.h>

#include "predicate.h"

namespace avro {
namespace predicate {


Predicate::Predicate(filter::equality_expression *expr) : expr(expr) {
}

Predicate::~Predicate() {
}

template<>
void Predicate::apply<StringBuffer>(const StringBuffer &sb) {

    const std::string & str = boost::get<std::string>(expr->constant);

    if (expr->op == filter::equality_expression::EQ) {
        expr->setState(sb == str);
        // std::cout << "state == " << (sb == str) << std::endl;
    } else if (expr->op == filter::equality_expression::NE) {
        expr->setState(sb != str);
        // std::cout << "state != " << (sb != str) << std::endl;
    } else if (expr->op == filter::equality_expression::STRING) {
        if (expr->strop == filter::string_operator::CONTAINS) {
            expr->setState(sb.contains(str));
        } else if (expr->strop == filter::string_operator::STARTS_WITH) {
            expr->setState(sb.starts_with(str));
        } else if (expr->strop == filter::string_operator::ENDS_WITH) {
            expr->setState(sb.ends_with(str));
        } else {
            assert(false && "expr->string_operator contains unknown operator");
        }
    } else {
        assert(false && "expr->op contains unknown operator");
    }
}

template<>
void Predicate::apply<int>(const int &i) {
    // TODO: use pattern "strategy" here
    if (expr->op == filter::equality_expression::EQ) {
        expr->setState(boost::get<int>(expr->constant) == i);
    } else if (expr->op == filter::equality_expression::NE) {
        expr->setState(boost::get<int>(expr->constant) != i);
    } else {
        assert(false && "expr->op contains unknown operator");
    }
}

template<>
void Predicate::apply<long>(const long &i) {
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