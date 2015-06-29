
#include <string>
#include <sstream>
#include <memory>
#include <vector>
#include <assert.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "node/node.h"
#include "node/array.h"
#include "node/custom.h"
#include "node/record.h"
#include "node/string.h"
#include "node/union.h"
#include "node/int.h"
#include "node/long.h"
#include "node/boolean.h"
#include "node/float.h"
#include "node/double.h"
#include "node/enum.h"
#include "node/map.h"
#include "node/null.h"

#include "schemareader.h"

namespace avro {

static bool isArray(const boost::property_tree::ptree &node);

static bool isObject(const boost::property_tree::ptree &node) {
    return !node.front().first.empty() && (
            !node.front().second.data().empty() ||
            isArray(node.front().second) ||
            isObject(node.front().second)
        );
}

static bool isArray(const boost::property_tree::ptree &node) {

    const auto &front = node.front();
    bool isOrdinaryValue = !front.second.data().empty();
    return front.first.empty() &&
        (
            isOrdinaryValue ||
            isArray(front.second) ||
            isObject(front.second)
        );

}

static bool isValue(const boost::property_tree::ptree &node) {
    return node.empty() && !node.data().empty();
}


class NodeDescriptor {
public:
    std::string type;
    std::string name;
    bool containsSymbols = false;
    bool containsFields = false;
    bool containsValues = false;
    bool containsItems = false;
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
        } else if (name == "symbols") {
            containsSymbols = true;
        } else if (name == "values") {
            containsValues = true;
        } else if (name == "items") {
            containsItems = true;
        } else {
            throw std::runtime_error("NodeDescriptor::assignField: unknown schema record name - '" + name + "'");
        }
    }
};

std::unique_ptr<Node> SchemaReader::nodeByTypeName(const std::string &typeName, const std::string &itemName) {
    if (typeName == "record") {
        return std::unique_ptr<Node>(new node::Record(currentIndex++, itemName));
    } else if (typeName == "enum") {
        return std::unique_ptr<Node>(new node::Enum(currentIndex++, itemName));
    } else if (typeName == "array") {
        return std::unique_ptr<Node>(new node::Array(currentIndex++, itemName));
    } else if (typeName == "string") {
        return std::unique_ptr<Node>(new node::String(currentIndex++, itemName));
    } else if (typeName == "bytes") { // TODO: make real type "bytes"
        return std::unique_ptr<Node>(new node::String(currentIndex++, itemName));
    } else if (typeName == "int") {
        return std::unique_ptr<Node>(new node::Int(currentIndex++, itemName));
    } else if (typeName == "long") {
        return std::unique_ptr<Node>(new node::Long(currentIndex++, itemName));
    } else if (typeName == "float") {
        return std::unique_ptr<Node>(new node::Float(currentIndex++, itemName));
    } else if (typeName == "double") {
        return std::unique_ptr<Node>(new node::Double(currentIndex++, itemName));
    } else if (typeName == "boolean") {
        return std::unique_ptr<Node>(new node::Boolean(currentIndex++, itemName));
    } else if (typeName == "null") {
        return std::unique_ptr<Node>(new node::Null(currentIndex++, itemName));
    } else if (typeName == "map") {
        return std::unique_ptr<Node>(new node::Map(currentIndex++, itemName));
    }
    throw std::runtime_error("unknown node type: '" + typeName + "' for node '" + itemName + "'");
}

SchemaReader::SchemaReader(const std::string &schemaJson) : schemaJson(schemaJson) {
}

SchemaReader::~SchemaReader() {
}

std::unique_ptr<Node> SchemaReader::parse() {
    std::istringstream ss(schemaJson);

    using boost::property_tree::ptree;
    boost::property_tree::ptree pt;
    boost::property_tree::read_json(ss, pt);

    return parseOneJsonObject(pt);
}

std::unique_ptr<Node> SchemaReader::parseOneJsonObject(const boost::property_tree::ptree &node) {

    auto descriptor = descriptorForJsonObject(node);

    std::unique_ptr<Node> newNode = nodeByType(node.get_child("type"), descriptor->name);
    assert(newNode.get() != nullptr);

    if (newNode->is<node::Record>()) {
        readRecordFields(node, newNode->as<node::Record>());
    } else if (newNode->is<node::Union>()) {
        newNode = readUnion(node.get_child("type"), descriptor->name);
    } else if (newNode->is<node::Array>()) {
        newNode->as<node::Array>().setItemsType(nodeByType(node.get_child("items"), descriptor->name));
    } else if (newNode->is<node::Map>()) {
        newNode->as<node::Map>().setItemsType(nodeByType(node.get_child("values"), descriptor->name));
    } else if (newNode->is<node::Enum>()) {
        readEnumValues(node, newNode->as<node::Enum>());
    }
    assert(newNode.get() != nullptr);
    return newNode;

}

std::unique_ptr<Node> SchemaReader::nodeByType(const boost::property_tree::ptree &type,
                                                     const std::string &nodeName) {
    if (isValue(type)) {
        return nodeByTypeName(type.data(), nodeName);
    } else if(isObject(type)) {
        auto newNode = std::unique_ptr<Node>(new node::Custom(currentIndex++, nodeName));
        if (newNode->is<node::Custom>()) {
            newNode->as<node::Custom>().setDefinition(parseOneJsonObject(type));
        }
        return newNode;
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
    auto unionNode = std::unique_ptr<node::Union>(new node::Union(currentIndex++, name));
    for(auto &p : node) {
        unionNode->addChild(nodeByType(p.second, name));
    }
    return unionNode; // TODO return good value
}

void SchemaReader::readEnumValues(const boost::property_tree::ptree &node, node::Enum &e) {
    for(const auto &p : node.get_child("symbols")) {
        e.addValue(p.second.data());
    }
}


int SchemaReader::nodesNumber() const {
    return currentIndex;
}


} /* namespace avro */
