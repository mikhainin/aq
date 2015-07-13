
#ifndef __avroq__equality_expression__
#define __avroq__equality_expression__

#include <string>
#include <boost/variant.hpp>

#include "nil.h"
#include "string_operator.h"

namespace filter {

    struct equality_expression
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


        /// state

        bool state = false;

        void resetState();
        void setState(bool newState);
    };

}
#endif /* defined(__avroq__equality_expression__) */
