//
//  main.cpp
//  avroq
//
//  Created by Mikhail Galanin on 09/11/14.
//  Copyright (c) 2014 Mikhail Galanin. All rights reserved.
//

#include <iostream>

#include "avro/reader.h"

int main(int argc, const char * argv[]) {
    
    avro::Reader reader(argv[1]);
    
    avro::header header = reader.readHeader();
    
    try {
        while (not reader.eof()) {
            reader.readBlock(header);
        }
    } catch (const avro::Reader::Eof &e) {
        ; // reading done
    }

    return 0;
}
