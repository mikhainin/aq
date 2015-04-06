#ifndef AVROQ_AVRO_SCHEMANODE_H_
#define AVROQ_AVRO_SCHEMANODE_H_

#include <string>
#include <memory>

namespace avro {

class Node {
public:
    Node(int number, const std::string &typeName, const std::string &itemName);
    virtual ~Node();

    template<class T>
    inline bool is() {
        return dynamic_cast<T*>(this) != nullptr;
    }

    template<class T>
    inline T& as() {
        return dynamic_cast<T&>(*this);
    }

    const std::string &getTypeName() const;

    const std::string &getItemName() const;

    int getNumber() const;
    //virtual void dump(std::function<void(std::string)> &dumper) = 0;

private:
    int number;
    std::string typeName;
    std::string itemName;
};

}


#endif /* AVROQ_AVRO_SCHEMANODE_H_ */
