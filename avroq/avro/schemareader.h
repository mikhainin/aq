/*
 * SchemaReader.h
 *
 *  Created on: 21 дек. 2014 г.
 *      Author: a
 */

#ifndef AVROQ_AVRO_SCHEMAREADER_H_
#define AVROQ_AVRO_SCHEMAREADER_H_

#include <boost/property_tree/ptree.hpp>

/*
namespace boost {
    namespace property_tree {
        class ptree; //::value_type
    }
}
*/

namespace avro {

class NodeDescriptor;
class Record;

class SchemaReader {
public:
    SchemaReader(const std::string &schemaJson);
    virtual ~SchemaReader();

    void parse();
private:
    const std::string &schemaJson;

    std::unique_ptr<NodeDescriptor> parseOneJsonObject(boost::property_tree::ptree &node);

    void readRecordFields(boost::property_tree::ptree &node, Record &record);
};

} /* namespace avro */

#endif /* AVROQ_AVRO_SCHEMAREADER_H_ */
