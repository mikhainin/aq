//
//  main.cpp
//  avroq
//
//  Created by Mikhail Galanin on 09/11/14.
//  Copyright (c) 2014 Mikhail Galanin. All rights reserved.
//

#include <iostream>
#include <string>

#include <boost/program_options.hpp>
//#include <boost/algorithm/string.hpp>
//#include <boost/filesystem.hpp>

// TODO: remove this include from this file
#include "avro/node/node.h"
#include "avro/reader.h"
#include "avro/eof.h"

namespace po = boost::program_options;



int main(int argc, const char * argv[]) {

    std::string condition;
    std::string what;

    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        // ("optimization", po::value<int>(&opt)->default_value(10),
      // "optimization level")
      //  ("include-path,I", po::value< std::vector<std::string> >(),
      //"include path")
        ("input-file,f", po::value< std::vector<std::string> >(), "input files")
        ("what,w", po::value< std::string >(&what), "what")
        ("condition,c", po::value< std::string >(&condition), "expression")
    ;
    po::positional_options_description p;
    p.add("input-file", -1);

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
              options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }


    if (vm.count("input-file")) {
        for(const auto &p : vm["input-file"].as< std::vector<std::string> >()) {

            std::cout << "Processing " << p << std::endl;

            avro::Reader reader(p);
            
            avro::header header = reader.readHeader();

            avro::FilterExpression filter;
            if (condition.size() > 0) {
                filter = reader.compileCondition(what, condition, header);
            }
            
            try {
                while (not reader.eof()) {
                    if (condition.size() > 0) {
                        reader.readBlock(header, &filter);
                    } else {
                        reader.readBlock(header);
                    }
                }
            } catch (const avro::Eof &e) {
                ; // reading done
            }
        }
    } else {
        std::cout << "No input files" << std::endl;
        return 1;
    }

    return 0;
}
