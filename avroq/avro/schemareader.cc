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
    
struct NodeDescriptor {
    std::string type;
    std::string name;
    bool containsFields = false;
    boost::property_tree::ptree node;
    
    NodeDescriptor(boost::property_tree::ptree node) : node(node){
    }
    
    void assignField(const std::string &name, const std::string &value) {
        if (name == "type") {
            type = value;
        } else if (name == "name") {
            this->name = value;
        } else if (name == "fields") {
            containsFields = true;
        } else {
            throw std::runtime_error("NodeDescriptor::assignField: unknown schema record name - " + name);
        }
    }
};

SchemaReader::SchemaReader(const std::string &schemaJson) {

    std::stringstream ss(schemaJson);

    boost::property_tree::ptree pt;
    boost::property_tree::read_json(ss, pt);

    auto &i = pt.front();

    NodeDescriptor descriptor(i);
    std::cout << i.first << ": " << i.second.data() << std::endl;

}

SchemaReader::~SchemaReader() {
    // TODO Auto-generated destructor stub
}

} /* namespace avro */
