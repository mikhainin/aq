
#include "ast.hpp"

namespace filter {
namespace detail {

expression_ast::expression_ast() : expr(nil()) {
}

expression_ast& expression_ast::operator&=(expression_ast const& rhs)
{
    expr = binary_op(binary_op::AND, expr, rhs);
    return *this;
}

expression_ast& expression_ast::operator|=(expression_ast const& rhs)
{
    expr = binary_op(binary_op::OR, expr, rhs);
    return *this;
}


expression_ast& expression_ast::operator != (expression_ast const& expr) {
	this->expr = not_op(expr);
	return *this;
}


binary_op::binary_op(
    OP op
  , expression_ast const& left
  , expression_ast const& right)
: op(op), left(left), right(right) {}


not_op::not_op(expression_ast const& expr) : expr(expr) {
}


} // namespace detail
} // namespace filter
