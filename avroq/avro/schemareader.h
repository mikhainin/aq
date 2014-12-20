/*
 * SchemaReader.h
 *
 *  Created on: 21 дек. 2014 г.
 *      Author: a
 */

#ifndef AVROQ_AVRO_SCHEMAREADER_H_
#define AVROQ_AVRO_SCHEMAREADER_H_

namespace avro {

class SchemaReader {
public:
    SchemaReader(const std::string &schemaJson);
    virtual ~SchemaReader();
};

} /* namespace avro */

#endif /* AVROQ_AVRO_SCHEMAREADER_H_ */
