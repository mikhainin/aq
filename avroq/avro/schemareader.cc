/*
 * SchemaReader.cc
 *
 *  Created on: 21 дек. 2014 г.
 *      Author: a
 */

#include <string>
#include <sstream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>


#include "schemareader.h"

namespace avro {

class SchemaNode {
public:
    virtual ~SchemaNode() {}
};

SchemaReader::SchemaReader(const std::string &schemaJson) {

    std::stringstream ss(schemaJson);

    boost::property_tree::ptree pt;
    boost::property_tree::read_json(ss, pt);

    auto &i = pt.front();

    std::cout << i.first << ": " << i.second.data() << std::endl;

}

SchemaReader::~SchemaReader() {
    // TODO Auto-generated destructor stub
}

} /* namespace avro */
