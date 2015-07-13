#ifndef _worker_h
#define _worker_h

class FileEmitor;

class Worker {
public:
    explicit Worker(FileEmitor &emitor);

    void operator()();
    
private:
    FileEmitor &emitor;
};

#endif