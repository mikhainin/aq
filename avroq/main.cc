
#include <iostream>
#include <string>

#include <thread>
#include <mutex>

#include <boost/program_options.hpp>

#include "avro/block.h"
#include "avro/blockdecoder.h"
#include "avro/reader.h"
#include "avro/eof.h"
#include "avro/finished.h"
#include "avro/limiter.h"
// TODO: remove this include (node.h) from this file as it's an implementation detail
#include "avro/node/node.h"

#include "filter/filter.h"
#include "filter/compiler.h"

namespace po = boost::program_options;

std::mutex screenMutex;

void outDocument(const std::string &what) {
    std::lock_guard<std::mutex> screenLock(screenMutex);
    std::cout.write(what.data(), what.size());
}

std::mutex readerMutex;

void processFile(avro::BlockDecoder &decoder, avro::Reader &reader, const avro::header &header) {
                    
    avro::Block block;
    try {
        while (true) {
            // TODO: use one/set of buffers
            {
                std::lock_guard<std::mutex> readerLock(readerMutex);
                if (reader.eof()) {
                    break;
                }
                reader.nextBlock(header, block);
            }
            decoder.decodeAndDumpBlock(block);
            //reader.readBlock(header, wd);
        }
    } catch (const avro::Eof &e) {
        ; // reading done
    } catch (const avro::Finished &e) {
        ;
    }

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

    if (vm.count("input-file")) {
        try {
            avro::Limiter limiter(limit);
            for(const auto &p : vm["input-file"].as< std::vector<std::string> >()) {

                if (printProcessingFile) {
                    std::cerr << "Processing " << p << std::endl;
                }

                avro::Reader reader(p, limiter);
                
                avro::header header = reader.readHeader();

                auto tsvFieldsList = reader.compileFieldsList(fields, header);

                if (!condition.empty()) {
                    filter = filterCompiler.compile(condition);
                    reader.setFilter(filter, header);
                }

                try {
                    avro::BlockDecoder decoder(header, limiter);
                    if (filter) {
                        decoder.setFilter(std::unique_ptr<filter::Filter>(new filter::Filter(*filter)));
                    }
                    decoder.setTsvFilterExpression(tsvFieldsList);
                    decoder.setDumpMethod(outDocument);
                    
                    std::vector<std::thread> workers;
                    for(u_int i = 0; i < jobs; ++i) { // TODO: check for inadequate values
                        workers.emplace_back(
                            std::thread(
                                std::bind(processFile, std::ref(decoder), std::ref(reader), std::ref(header))
                            )
                        );
                    }
                    for(auto &p : workers) {
                        p.join();
                    }
                } catch (const avro::Eof &e) {
                    ; // reading done
                }
                if (limiter.finished()) {
                    break;
                }
            }
        } catch (const avro::Reader::PathNotFound &e) {
        	std::cerr << "can't locate path '" << e.getPath() << "'" << std::endl;
        } catch (const std::runtime_error &e) {
            std::cerr << e.what() << std::endl;
        }

    } else {
        std::cerr << "No input files" << std::endl;
        return 1;
    }

    return 0;
}
