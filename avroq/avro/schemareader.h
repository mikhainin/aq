/*
 * SchemaReader.h
 *
 *  Created on: 21 дек. 2014 г.
 *      Author: a
 */

#ifndef AVROQ_AVRO_SCHEMAREADER_H_
#define AVROQ_AVRO_SCHEMAREADER_H_

#include <boost/property_tree/ptree.hpp>

namespace avro {

namespace node {
    class Record;
    class Union;
    class Custom;
}
class NodeDescriptor;
class Node;

class SchemaReader {
public:
    SchemaReader(const std::string &schemaJson);
    virtual ~SchemaReader();

    std::unique_ptr<Node> parse();

    int nodesNumber() const;
private:
    const std::string &schemaJson;
    int currentIndex = 0;

    std::unique_ptr<Node> parseOneJsonObject(const boost::property_tree::ptree &node);
    std::unique_ptr<NodeDescriptor> descriptorForJsonObject(const boost::property_tree::ptree &node);
    std::unique_ptr<Node> NodeByTypeName(const std::string &typeName, const std::string &itemName);

    void readRecordFields(const boost::property_tree::ptree &node, node::Record &record);
    void readRecordDefinition(const boost::property_tree::ptree &node, node::Custom &record);
    std::unique_ptr<node::Union> readUnion(const boost::property_tree::ptree &node, const std::string &name);
    
    std::unique_ptr<Node> nodeByType(const boost::property_tree::ptree &node,
                                     const std::string &nodeName);
};

} /* namespace avro */

#endif /* AVROQ_AVRO_SCHEMAREADER_H_ */
