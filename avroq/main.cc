
#include <iostream>
#include <string>

#include <boost/program_options.hpp>

// TODO: remove this include (node.h) from this file
#include "avro/node/node.h"
#include "avro/reader.h"
#include "avro/eof.h"
#include "avro/finished.h"
#include "avro/limiter.h"

namespace po = boost::program_options;



int main(int argc, const char * argv[]) {

    std::string condition;
    std::string what;
    int limit = -1;
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("input-file,f", po::value< std::vector<std::string> >(), "input files")
        ("what,w", po::value< std::string >(&what), "what")
        ("condition,c", po::value< std::string >(&condition), "expression")
        ("limit,n", po::value< int >(&limit)->default_value(-1), "maximum number of records (default -1 means no limit")
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
        try {
            avro::Limiter limiter(limit);
            for(const auto &p : vm["input-file"].as< std::vector<std::string> >()) {

                std::cout << "Processing " << p << std::endl;

                avro::Reader reader(p, limiter);
                
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
        } catch (const avro::Finished &e) {
            ;
        }

    } else {
        std::cout << "No input files" << std::endl;
        return 1;
    }

    return 0;
}
