
#include <string>
#include <sstream>
#include <memory>
#include <vector>
#include <assert.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "schemanode.h"

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

class RecordNode : public SchemaNode {
public:
    RecordNode();

    void addChild(std::unique_ptr<SchemaNode> &&newChild);
private:
    std::vector<std::unique_ptr<SchemaNode> > children;
};
    
RecordNode::RecordNode()
: SchemaNode("record") {
}

void RecordNode::addChild(std::unique_ptr<SchemaNode> &&newChild) {
    children.push_back(std::move(newChild));
}

class EnumNode : public SchemaNode {
public:
    explicit EnumNode();
    void addElement(std::unique_ptr<SchemaNode> definition);
private:
    std::vector<std::unique_ptr<SchemaNode>> elements;
};

EnumNode::EnumNode() : SchemaNode("enum") {
}

void EnumNode::addElement(std::unique_ptr<SchemaNode> definition) {
    elements.emplace_back(std::move(definition));
}

class CustomType : public SchemaNode {
public:
    CustomType(const std::string &typeName);

    void setDefinition(std::unique_ptr<SchemaNode> definition);
private:
    std::unique_ptr<SchemaNode> definition;
};
    
CustomType::CustomType(const std::string &typeName) : SchemaNode(typeName) {
}

void CustomType::setDefinition(std::unique_ptr<SchemaNode> definition) {
    this->definition = std::move(definition);
}

class StringNode : public SchemaNode {
public:
    StringNode();

private:
};

StringNode::StringNode()
    : SchemaNode("string") {
}

class DoubleNode : public SchemaNode {
public:
    DoubleNode() : SchemaNode("double") {
    }
};

class IntNode : public SchemaNode {
public:
    IntNode() : SchemaNode("int") {
    }
};

class NullNode : public SchemaNode {
public:
    NullNode() : SchemaNode("null") {
    }
};

// TODO: move to member
static std::unique_ptr<SchemaNode> NodeByName(const std::string &name) {
    std::cout << name << std::endl;
    if (name == "record") {
        return std::unique_ptr<SchemaNode>(new RecordNode());
    } else if (name == "string") {
        return std::unique_ptr<SchemaNode>(new StringNode());
    } else if (name == "int") {
        return std::unique_ptr<SchemaNode>(new IntNode());
    }
    /*} else if (name == "" && descriptor.isCustomTypeDefinition) {
        // custom type
        // TODO: register custom type
        return std::unique_ptr<SchemaNode>(new CustomType(descriptor));
    }*/
    throw std::runtime_error("unknown node type: '" + name + "'");
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

    std::unique_ptr<SchemaNode> newNode = nodeByType(node.get_child("type"));

    if (newNode->is<RecordNode>()) {
        readRecordFields(node, dynamic_cast<RecordNode &>(*newNode));
    } else if (newNode->is<CustomType>()) {
        dynamic_cast<CustomType&>(*newNode).setDefinition(parseOneJsonObject(node.get_child("type")));
    }

    return newNode;

}

std::unique_ptr<SchemaNode> SchemaReader::nodeByType(const boost::property_tree::ptree &type) {
    if (isValue(type)) {
        return NodeByName(type.data());
    } else if(isObject(type)) {
        return std::unique_ptr<SchemaNode>(new CustomType("custom")); // parseEnum()->name
        // record.setDefinition(parseOneJsonObject(pt));
    } else if (isArray(type)) {
        return std::unique_ptr<SchemaNode>(new EnumNode());
        // readEnum(pt);
    } else {
        // std::cout << pt.front().first << ":" << pt.front().second.data() << std::endl;
        throw std::runtime_error("smth wrong with 'type'");
    }
}

std::unique_ptr<NodeDescriptor> SchemaReader::descriptorForJsonObject(const boost::property_tree::ptree &node) {

    std::unique_ptr<NodeDescriptor> descriptor(new NodeDescriptor(node));

    for(const auto &p : node) {
        descriptor->assignField(p.first, p.second.data());
    }

    return descriptor;

}

void SchemaReader::readRecordFields(const boost::property_tree::ptree &node, RecordNode &record) {
    for(const auto &p : node.get_child("fields")) {
        auto newNode = parseOneJsonObject(p.second);
        record.addChild(std::move(newNode));// TODO: add node name
    }
}

std::unique_ptr<EnumNode> SchemaReader::readEnum(const boost::property_tree::ptree &node) {
    auto enumNode = std::unique_ptr<EnumNode>(new EnumNode());
    for(auto &p : node) {
        enumNode->addElement(nodeByType(p.second));
        // std::cout << p.second.data() << std::endl;
    }
    return enumNode; // TODO return good value
}


} /* namespace avro */
