/*
 * SchemaReader.cc
 *
 *  Created on: 21 дек. 2014 г.
 *      Author: a
 */

#include <string>
#include <sstream>
#include <memory>

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

class Record : public SchemaNode {
public:
    Record(const NodeDescriptor &descriptor);
private:
    const std::string &name;
    std::vector<std::unique_ptr<SchemaNode> > children;
};
Record::Record(const NodeDescriptor &descriptor)
    : name(descriptor.name) {

}


// TODO: move to member
static std::unique_ptr<SchemaNode> NodeByName(const NodeDescriptor &descriptor) {
    if (descriptor.type == "record") {
        return std::unique_ptr<SchemaNode>(new Record(descriptor));
    }
    throw std::runtime_error("unknown node type: " + descriptor.type);
}

SchemaReader::SchemaReader(const std::string &schemaJson) : schemaJson(schemaJson) {
}

SchemaReader::~SchemaReader() {
}

void SchemaReader::parse() {
    std::stringstream ss(schemaJson);

    boost::property_tree::ptree pt;
    boost::property_tree::read_json(ss, pt);

    // std::cout << "type: " << pt.get<std::string>("type") << std::endl;
/*
    for(const auto &p : pt) {
        NodeDescriptor descriptor(p);
        std::cout << p.first << ": " << p.second.data() << std::endl;
        // auto newNode = NodeByName(descriptor);
    }*/

    auto descriptor = parseOneJsonObject(pt);
    auto newNode = NodeByName(*descriptor);

    if (descriptor->containsFields) {
        readRecordFields(pt, dynamic_cast<Record &>(*newNode));
    }
}

std::unique_ptr<NodeDescriptor> SchemaReader::parseOneJsonObject(boost::property_tree::ptree &node) {

    std::unique_ptr<NodeDescriptor> descriptor(new NodeDescriptor(node));

    for(const auto &p : node) {
        descriptor->assignField(p.first, p.second.data());
    }

    return descriptor;
    // auto newNode = NodeByName(descriptor);

}

void SchemaReader::readRecordFields(boost::property_tree::ptree &node, Record &record) {
    for(const auto &p : node.get_child("fields")) {
        parseOneJsonObject(p.second.get_child());
    }
}

} /* namespace avro */
