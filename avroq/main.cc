
#include <iostream>
#include <string>

#include <thread>
#include <mutex>

#include <boost/program_options.hpp>

// TODO: remove this include (node.h) from this file as it's an implementation detail
#include "avro/node/node.h"

#include "filter/filter.h"
#include "filter/compiler.h"

#include "fileemitor.h"
#include "worker.h"
namespace po = boost::program_options;

std::mutex screenMutex;

void outDocument(const std::string &what) {
    std::lock_guard<std::mutex> screenLock(screenMutex);
    std::cout.write(what.data(), what.size());
}

int main(int argc, const char * argv[]) {

    std::cout.sync_with_stdio(false);

    std::string condition;
    int limit = -1;
    u_int jobs = 4;
    std::string fields;
    bool printProcessingFile = false;

    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("input-file,f", po::value< std::vector<std::string> >(), "input files")
        ("condition,c", po::value< std::string >(&condition), "expression")
        ("limit,n", po::value< int >(&limit)->default_value(-1), "maximum number of records (default -1 means no limit)")
        ("fields,l", po::value< std::string >(&fields), "fields to output")
        ("print-file", po::bool_switch(&printProcessingFile), "Print name of processing file")
        ("jobs,j", po::value< u_int >(&jobs), "Number of threads to run")
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

    if (!condition.empty()) {
        try {
            filter = filterCompiler.compile(condition);
        } catch(const filter::Compiler::CompileError &e) {
            std::cerr << "Condition complile failed: " << std::endl;
            std::cerr << e.what() << std::endl;
            return 1;
        }
    }

    if (vm.count("input-file")) {

        const auto &fileList = vm["input-file"].as< std::vector<std::string> >();

        FileEmitor emitor(fileList, limit, outDocument);

        emitor.setFilter(filter);
        emitor.setTsvFieldList(fields);
        if (printProcessingFile) {
            emitor.enablePrintProcessingFile();
        }
        std::vector<std::thread> workers;

        for(u_int i = 0; i < jobs; ++i) { // TODO: check for inadequate values
            workers.emplace_back(
                    std::thread(Worker(emitor))
                );
        }

        for(auto &p : workers) {
            p.join();
        }

    } else {
        std::cerr << "No input files" << std::endl;
        return 1;
    }

    return 0;
}
