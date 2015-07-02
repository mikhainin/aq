
#ifndef __avroq__equality_expression__
#define __avroq__equality_expression__

#include <string>
#include <boost/variant.hpp>

#include "nil.h"

namespace filter {

    struct equality_expression
    {
        enum OP {
            EQ,
            NE
        };
        typedef
            boost::variant<
                nil
              , int
              , std::string
            > type;

        equality_expression();

        equality_expression(const std::string &ident);

        equality_expression& operator == (const type &constant);

        equality_expression& operator != (const type &constant);

        type constant;
        std::string identifier;
        OP op;


        /// state

        bool state = false;

        void resetState();
        void setState(bool newState);
    };

}
#endif /* defined(__avroq__equality_expression__) */
