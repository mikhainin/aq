//
//  main.cpp
//  avroq
//
//  Created by Mikhail Galanin on 09/11/14.
//  Copyright (c) 2014 Mikhail Galanin. All rights reserved.
//

#include <iostream>

#include "avroreader.h"

int main(int argc, const char * argv[]) {
    
    AvroReader reader;
    
    reader.readFile(argv[1]);
    
    return 0;
}
