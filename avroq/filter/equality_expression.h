
#ifndef __avroq__equality_expression__
#define __avroq__equality_expression__

#include <string>
#include <vector>
#include <boost/variant.hpp>

#include "nil.h"
#include "state.h"
#include "string_operator.h"

namespace filter {

struct record_expression;

namespace detail {
    struct array_element;
}
    struct equality_expression : public state
    {
        enum OP {
            EQ,
            NE,
            LT,
            GT,
            LE,
            GE,
            STRING,
            IS_NIL,
            NOT_NIL
        };
        typedef
            boost::variant<
                filter::nil
              , int
              , bool
              , double
              , float
              , std::string
            > type;

        equality_expression();

        equality_expression(const std::string &ident);
        equality_expression(const detail::array_element &ident);

        equality_expression& operator == (const type &constant);

        equality_expression& operator != (const type &constant);

        equality_expression& operator |= (const string_operator &constant);
        equality_expression& operator |= (const type &constant);

        equality_expression& operator < (const type &constant);
        equality_expression& operator > (const type &constant);
        equality_expression& operator >= (const type &constant);
        equality_expression& operator <= (const type &constant);

        type constant;
        std::string identifier;
        OP op;
        string_operator::ops_t strop;
        /*
        bool is_array_element = false;
        size_t array_index = 0;
        std::vector<bool> array_states;
*/

        /// state
/*
        bool state = false;

        void resetState();
        void setState(bool newState);
        bool getState() const;
        void pushState();
*/
        record_expression * parent = nullptr;
        void setParent(record_expression * parent);
    };

}
#endif /* defined(__avroq__equality_expression__) */
