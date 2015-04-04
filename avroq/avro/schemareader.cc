
#include <string>
#include <sstream>
#include <memory>
#include <vector>
#include <assert.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "node/schemanode.h"
#include "node/custom.h"
#include "node/record.h"
#include "node/string.h"
#include "node/union.h"
#include "node/int.h"
#include "node/boolean.h"
#include "node/float.h"
#include "node/double.h"
#include "node/null.h"
#include "schemareader.h"

namespace avro {


static bool isObject(const boost::property_tree::ptree &node) {
    return !node.front().first.empty() && !node.front().second.data().empty();
}

static bool isArray(const boost::property_tree::ptree &node) {
    return node.front().first.empty() && !node.front().second.data().empty();
}

static bool isValue(const boost::property_tree::ptree &node) {
    return node.empty() && !node.data().empty();
}


class NodeDescriptor {
public:
    std::string type;
    std::string name;
    bool containsFields = false;
    bool isCustomTypeDefinition = false;
    boost::property_tree::ptree node;

    NodeDescriptor(const boost::property_tree::ptree &node) : node(node){
    }
    
    void assignField(const std::string &name, const std::string &value) {
        
        std::cout << name << "=>'" << value << "'" <<  std::endl;
        if (name == "type" && value == "") {
            isCustomTypeDefinition = true; // type is an object (or probably array?)
        } else if (name == "type") {
            type = value;
        } else if (name == "name") {
            this->name = value;
        } else if (name == "fields") {
            containsFields = true;
        } else {
            throw std::runtime_error("NodeDescriptor::assignField: unknown schema record name - '" + name + "'");
        }
    }
};

// TODO: move to member
static std::unique_ptr<SchemaNode> NodeByTypeName(const std::string &typeName, const std::string &itemName) {
    // std::cout << typeName << std::endl;
    if (typeName == "record") {
        return std::unique_ptr<SchemaNode>(new node::Record(itemName));
    } else if (typeName == "string") {
        return std::unique_ptr<SchemaNode>(new node::String(itemName));
    } else if (typeName == "int") {
        return std::unique_ptr<SchemaNode>(new node::Int(itemName));
    } else if (typeName == "float") {
        return std::unique_ptr<SchemaNode>(new node::Float(itemName));
    } else if (typeName == "double") {
        return std::unique_ptr<SchemaNode>(new node::Double(itemName));
    } else if (typeName == "boolean") {
        return std::unique_ptr<SchemaNode>(new node::Boolean(itemName));
    } else if (typeName == "null") {
        return std::unique_ptr<SchemaNode>(new node::Null());
    }
    /*} else if (name == "" && descriptor.isCustomTypeDefinition) {
        // custom type
        // TODO: register custom type
        return std::unique_ptr<SchemaNode>(new CustomType(descriptor));
    }*/
    throw std::runtime_error("unknown node type: '" + typeName + "' for node '" + itemName + "'");
}

SchemaReader::SchemaReader(const std::string &schemaJson) : schemaJson(schemaJson) {
}

SchemaReader::~SchemaReader() {
}

std::unique_ptr<SchemaNode> SchemaReader::parse() {
    std::stringstream ss(schemaJson);

    boost::property_tree::ptree pt;
    boost::property_tree::read_json(ss, pt);

    return parseOneJsonObject(pt);
}

std::unique_ptr<SchemaNode> SchemaReader::parseOneJsonObject(const boost::property_tree::ptree &node) {

    auto descriptor = descriptorForJsonObject(node);

    std::unique_ptr<SchemaNode> newNode = nodeByType(node.get_child("type"), descriptor->name);

    if (newNode->is<node::Record>()) {
        readRecordFields(node, dynamic_cast<node::Record &>(*newNode));
    } else if (newNode->is<node::Custom>()) {
        std::cout << "set def " << newNode->getTypeName() << std::endl  ;
        newNode->as<node::Custom>().setDefinition(parseOneJsonObject(node.get_child("type")));
        std::cout << "def for " <<  newNode->getTypeName() << " = " << newNode->as<node::Custom>().getDefinition()->getItemName() << std::endl;
    } else if (newNode->is<node::Union>()) {
        newNode = readUnion(node.get_child("type"), descriptor->name);
    } else {
        // return NodeByTypeName(type.data(), nodeName);
        // throw std::runtime_error("can't determine type for node '" + descriptor->name + "':'" + descriptor->type + "'");
    }

    return newNode;

}

std::unique_ptr<SchemaNode> SchemaReader::nodeByType(const boost::property_tree::ptree &type,
                                                     const std::string &nodeName) {
    if (isValue(type)) {
        return NodeByTypeName(type.data(), nodeName);
    } else if(isObject(type)) {
        return std::unique_ptr<SchemaNode>(new node::Custom(nodeName)); // parseEnum()->name
        // record.setDefinition(parseOneJsonObject(pt));
    } else if (isArray(type)) {
        return std::unique_ptr<SchemaNode>(new node::Union(nodeName));
    } else {
        // std::cout << pt.front().first << ":" << pt.front().second.data() << std::endl;
        throw std::runtime_error("smth wrong with 'type' for node '" + nodeName + "'");
    }
}

std::unique_ptr<NodeDescriptor> SchemaReader::descriptorForJsonObject(const boost::property_tree::ptree &node) {

    std::unique_ptr<NodeDescriptor> descriptor(new NodeDescriptor(node));

    for(const auto &p : node) {
        descriptor->assignField(p.first, p.second.data());
    }

    return descriptor;

}

void SchemaReader::readRecordFields(const boost::property_tree::ptree &node, node::Record &record) {
    for(const auto &p : node.get_child("fields")) {
        auto newNode = parseOneJsonObject(p.second);
        record.addChild(std::move(newNode));// TODO: add node name
    }
}

std::unique_ptr<node::Union> SchemaReader::readUnion(const boost::property_tree::ptree &node, const std::string &name) {
    auto enumNode = std::unique_ptr<node::Union>(new node::Union(name));
    for(auto &p : node) {
        enumNode->addChild(nodeByType(p.second, name));
        // std::cout << p.second.data() << std::endl;
    }
    return enumNode; // TODO return good value
}

void SchemaReader::dumpSchema(std::unique_ptr<SchemaNode> schema, std::ostream &to) {
    
}

} /* namespace avro */
