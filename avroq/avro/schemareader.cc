
#include <string>
#include <sstream>
#include <memory>
#include <vector>
#include <assert.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "node/Node.h"
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
std::unique_ptr<Node> SchemaReader::NodeByTypeName(const std::string &typeName, const std::string &itemName) {
    if (typeName == "record") {
        return std::unique_ptr<Node>(new node::Record(currentIndex++, itemName));
    } else if (typeName == "string") {
        return std::unique_ptr<Node>(new node::String(currentIndex++, itemName));
    } else if (typeName == "int") {
        return std::unique_ptr<Node>(new node::Int(currentIndex++, itemName));
    } else if (typeName == "float") {
        return std::unique_ptr<Node>(new node::Float(currentIndex++, itemName));
    } else if (typeName == "double") {
        return std::unique_ptr<Node>(new node::Double(currentIndex++, itemName));
    } else if (typeName == "boolean") {
        return std::unique_ptr<Node>(new node::Boolean(currentIndex++, itemName));
    } else if (typeName == "null") {
        return std::unique_ptr<Node>(new node::Null(currentIndex++));
    }
    throw std::runtime_error("unknown node type: '" + typeName + "' for node '" + itemName + "'");
}

SchemaReader::SchemaReader(const std::string &schemaJson) : schemaJson(schemaJson) {
}

SchemaReader::~SchemaReader() {
}

std::unique_ptr<Node> SchemaReader::parse() {
    std::stringstream ss(schemaJson);

    boost::property_tree::ptree pt;
    boost::property_tree::read_json(ss, pt);

    return parseOneJsonObject(pt);
}

std::unique_ptr<Node> SchemaReader::parseOneJsonObject(const boost::property_tree::ptree &node) {

    auto descriptor = descriptorForJsonObject(node);

    std::unique_ptr<Node> newNode = nodeByType(node.get_child("type"), descriptor->name);

    if (newNode->is<node::Record>()) {
        readRecordFields(node, dynamic_cast<node::Record &>(*newNode));
    } else if (newNode->is<node::Custom>()) {
        newNode->as<node::Custom>().setDefinition(parseOneJsonObject(node.get_child("type")));
    } else if (newNode->is<node::Union>()) {
        newNode = readUnion(node.get_child("type"), descriptor->name);
    }

    return newNode;

}

std::unique_ptr<Node> SchemaReader::nodeByType(const boost::property_tree::ptree &type,
                                                     const std::string &nodeName) {
    if (isValue(type)) {
        return NodeByTypeName(type.data(), nodeName);
    } else if(isObject(type)) {
        return std::unique_ptr<Node>(new node::Custom(currentIndex++, nodeName)); // parseEnum()->name
    } else if (isArray(type)) {
        return std::unique_ptr<Node>(new node::Union(currentIndex++, nodeName));
    } else {
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
    auto enumNode = std::unique_ptr<node::Union>(new node::Union(currentIndex++, name));
    for(auto &p : node) {
        enumNode->addChild(nodeByType(p.second, name));
    }
    return enumNode; // TODO return good value
}

int SchemaReader::nodesNumber() const {
    return currentIndex;
}


} /* namespace avro */
