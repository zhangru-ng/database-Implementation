#ifndef THREAD_H
#define THREAD_H

#include <pthread.h>
#include <iostream>
#include <stdlib.h>

class Thread {

private:
    pthread_t _thread;
	//C++ class member functions have a hidden this parameter passed in, 
	//use a static class method (which has no this parameter) to bootstrap the class
    static void * InternalThreadEntryFunc(void * This) {
        static_cast<Thread *>(This)->InternalThreadEntry(); 
        return nullptr;
    }
   	
protected:
   /** Implement this method in your subclass with the code you want your thread to run. */
    virtual void* InternalThreadEntry() = 0;  	

public:	
    void StartInternalThread(){
        if( 0 != pthread_create(&_thread, nullptr, InternalThreadEntryFunc, this)){
            cerr << "ERROR: Fail to start new thread!\n";
            exit(1);
        }
    }
    /** Will not return until the internal thread has exited. */
    void WaitForInternalThreadToExit(){
        pthread_join(_thread, nullptr);
    }
};

#endif
