
#include <iostream>
#include <string>

#include <thread>
#include <mutex>

#include <boost/program_options.hpp>

#include "filter/compiler.h"

#include "fileemitor.h"
#include "version.h"
#include "worker.h"
namespace po = boost::program_options;

std::mutex screenMutex;

std::string recordSeparator = "\n";
void outDocument(const std::string &what) {
    std::lock_guard<std::mutex> screenLock(screenMutex);
    std::cout.write(what.data(), what.size());
    std::cout << recordSeparator;
}

void updateSeparator(std::string &sep) {
    std::string res;
    bool esc = false;
    for(char c : sep) {
        if (esc) {
            switch(c) {
                case 'n':
                    res += '\n';
                    break;
                case 't':
                    res += '\t';
                    break;
                default:
                    res += c;
            }
            esc = false;
            continue;
        }
        if (c == '\\') {
            esc = true;
        } else {
            res += c;
        }
    }
    sep = res;
}

void correctJobsNumber(u_int &jobs) {
    if (jobs < 1) {
        std::cerr << "hint: adjusing threads number to 1" << std::endl;
        jobs = 1;
    } else if (jobs > 10) {
        std::cerr << "hint: do not be so greedy. adjusing threads number to 10" << std::endl;
        jobs = 1;
    }
}

int main(int argc, const char * argv[]) {

    std::cout.sync_with_stdio(false);

    std::string condition;
    int limit = -1;
    u_int jobs = 1;
    std::string fields;
    bool printProcessingFile = false;
    bool countMode = false;
    bool disableParseLoop = false;
    bool displayVersionAndExit = false;
    std::string fieldSeparator = "\t";

    po::options_description desc("Allowed options");
    desc.add_options()
        ("input-file,f", po::value< std::vector<std::string> >(), "Input files")
        ("condition,c", po::value< std::string >(&condition), "Expression")
        ("limit,n", po::value< int >(&limit)->default_value(-1), "Maximum number of records (default -1 means no limit)")
        ("fields,l", po::value< std::string >(&fields), "Fields to output")
        ("print-file", po::bool_switch(&printProcessingFile), "Print name of processing file")
        ("jobs,j", po::value< u_int >(&jobs), "Number of threads to run")
        ("count-only", po::bool_switch(&countMode), "Count of matched records, don't print them")
        ("record-separator", po::value<std::string>(&recordSeparator)->default_value("\\n"), "Record separator (\\n by default)")
        ("field-separator", po::value<std::string>(&fieldSeparator)->default_value("\\t"), "Field separator for TSV output (\\t by default)")
        ("disable-parse-loop", po::bool_switch(&disableParseLoop), "Disable experimental parsing mode (enabled by default)")
        ("help,h", "Show help message")
        ("version", po::bool_switch(&displayVersionAndExit), "Display version and exit")
    ;

    po::positional_options_description p;
    p.add("input-file", -1);

    po::variables_map vm;
    try {
        po::store(po::command_line_parser(argc, argv).
                  options(desc).positional(p).run(), vm);
        po::notify(vm);
    } catch(const boost::program_options::error &e) {
        std::cerr << "Sorry, I couldn't parse arguments: " << e.what() << std::endl;
        return 1;
    }
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    if (displayVersionAndExit) {
        std::cout << "aq version: " << avroqVersion() << std::endl;
        return 0;
    }

    updateSeparator(recordSeparator);
    updateSeparator(fieldSeparator);

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
        emitor.setTsvFieldList(fields, fieldSeparator);
        if (printProcessingFile) {
            emitor.enablePrintProcessingFile();
        }
        if (countMode) {
            emitor.enableCountOnlyMode();
        }
        if ( ! disableParseLoop ) {
            emitor.enableParseLoop();
        }
        std::vector<std::thread> workers;

        correctJobsNumber(jobs);

        for(u_int i = 0; i < jobs; ++i) { // TODO: check for inadequate values
            workers.emplace_back(
                    std::thread(Worker(emitor))
                );
        }

        for(auto &p : workers) {
            p.join();
        }

        if (countMode) {
            std::cout << "Matched documents: " << emitor.getCountedDocuments() << std::endl;
        }

    } else {
        std::cerr << "No input files" << std::endl;
        return 1;
    }

    return 0;
}
