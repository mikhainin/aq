
#include <iostream>
#include <string>

#include <boost/program_options.hpp>

#include "avro/reader.h"
#include "avro/eof.h"
#include "avro/finished.h"
#include "avro/limiter.h"
// TODO: remove this include (node.h) from this file as it's an implementation detail
#include "avro/node/node.h"

#include "filter/filter.h"
#include "filter/compiler.h"

namespace po = boost::program_options;



int main(int argc, const char * argv[]) {

    std::cout.sync_with_stdio(false);

    std::string condition;
    std::string what;
    int limit = -1;
    std::string fields;

    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("input-file,f", po::value< std::vector<std::string> >(), "input files")
        ("what,w", po::value< std::string >(&what), "what")
        ("condition,c", po::value< std::string >(&condition), "expression")
        ("limit,n", po::value< int >(&limit)->default_value(-1), "maximum number of records (default -1 means no limit)")
        ("fields,l", po::value< std::string >(&fields), "fields to output (order is not preserverd YET)")
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

    filter::Compiler filterCompiler;
    std::shared_ptr<filter::Filter> filter;

    if (vm.count("input-file")) {
        try {
            avro::Limiter limiter(limit);
            for(const auto &p : vm["input-file"].as< std::vector<std::string> >()) {

                std::cerr << "Processing " << p << std::endl;

                avro::Reader reader(p, limiter);
                
                avro::header header = reader.readHeader();

                // avro::FilterExpression filter;

                auto wd = reader.compileFieldsList(fields, header);

                /* if (condition.size() > 0) {
                    filter = reader.compileCondition(what, condition, header);
                } */
                if (!condition.empty()) {
                    filter = filterCompiler.compile(condition);
                    reader.setFilter(filter, header);
                }

                try {
                    while (not reader.eof()) {
                        /*if (condition.size() > 0) {
                            // std::cout << "main " << wd.what.size() << std::endl;
                            reader.readBlock(header, &filter, wd);
                        } else {*/
                            reader.readBlock(header, nullptr, wd);
                        //}
                    }
                } catch (const avro::Eof &e) {
                    ; // reading done
                }
            }
        } catch (const avro::Finished &e) {
            ;
        } catch (const avro::Reader::PathNotFound &e) {
        	std::cerr << "can't locate path" << e.getPath() << std::endl;
        }

    } else {
        std::cerr << "No input files" << std::endl;
        return 1;
    }

    return 0;
}
