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

#include "schemanode.h"

#include "schemareader.h"

namespace avro {

struct NodeDescriptor {
    std::string type;
    std::string name;
    bool containsFields = false;
    boost::property_tree::ptree node;

    NodeDescriptor(const boost::property_tree::ptree &node) : node(node){
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

class RecordNode : public SchemaNode {
public:
    RecordNode(const NodeDescriptor &descriptor);

    void addChild(std::unique_ptr<SchemaNode> &&newChild);
private:
    std::vector<std::unique_ptr<SchemaNode> > children;
};

RecordNode::RecordNode(const NodeDescriptor &descriptor)
    : SchemaNode(descriptor.name) {
}

void RecordNode::addChild(std::unique_ptr<SchemaNode> &&newChild) {
    children.push_back(std::move(newChild));
}

class StringNode : public SchemaNode {
public:
    StringNode(const NodeDescriptor &descriptor);

private:
};

StringNode::StringNode(const NodeDescriptor &descriptor)
    : SchemaNode(descriptor.name) {
}


// TODO: move to member
static std::unique_ptr<SchemaNode> NodeByName(const NodeDescriptor &descriptor) {
    std::cout << descriptor.type << std::endl;
    if (descriptor.type == "record") {
        return std::unique_ptr<SchemaNode>(new RecordNode(descriptor));
    } else if (descriptor.type == "string") {
        return std::unique_ptr<SchemaNode>(new StringNode(descriptor));
    }
    throw std::runtime_error("unknown node type: " + descriptor.type);
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

    auto newNode = NodeByName(*descriptor);

    if (descriptor->containsFields) {
        readRecordFields(node, dynamic_cast<RecordNode &>(*newNode));
    }

    return newNode;

}

std::unique_ptr<NodeDescriptor> SchemaReader::descriptorForJsonObject(const boost::property_tree::ptree &node) {

    std::unique_ptr<NodeDescriptor> descriptor(new NodeDescriptor(node));

    for(const auto &p : node) {
        descriptor->assignField(p.first, p.second.data());
    }

    return descriptor;
    // auto newNode = NodeByName(descriptor);

}

void SchemaReader::readRecordFields(const boost::property_tree::ptree &node, RecordNode &record) {
    for(const auto &p : node.get_child("fields")) {

        auto newNode = parseOneJsonObject(p.second);
/*

        if (newNode->is<Record>()) {
        // if (descriptor->containsFields) {
            readRecordFields(p.second, dynamic_cast<Record &>(*newNode));
        }
*/

        record.addChild(std::move(newNode));
    }
}

} /* namespace avro */
