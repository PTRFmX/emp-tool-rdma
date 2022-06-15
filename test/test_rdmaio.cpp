#include "emp-tool/io/rdma_io_channel.h"
#include <iostream>
#include <string>
#include <cstdio>

using namespace emp;


char* generate_random_block(size_t size, uint32_t seed) {
    srand(seed);
    char* block = new char[size];
    for (size_t i = 0; i < size; ++i) {
        block[i] = rand() % 256;
    }
    // printf("generate at 0: %d\n", block[0]);
    return block;
}

bool test_sndrecv(RDMAIO *io, uint32_t role, char *block, char *buffer) {
    if (role == 1)
        io->send_data(block, 1024); // client send data
    else
        io->recv_data((void *)buffer, 1024); // server recv data
    io->sync();
    if (role == 0) { // compare received data
        for (uint i = 0; i < 1024; ++i) {
            if (block[i] != buffer[i]) {
                printf("error at %d\n", i);
                printf("shoud be %d, but is %d\n", block[i], buffer[i]);
                return false;
            }
        }
    }
    printf("success\n");
    return true;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        std::cout << "Usage: ./test_rdmaio <role> <host> <port>" << std::endl;
        return -1;
    }
    uint role = atoi(argv[1]);
    std::string host = argv[2];
    uint port = atoi(argv[3]);

    RDMAIO *io = new RDMAIO(role == 0? nullptr: host.c_str(), port);

    char *buffer = new char[1024];
    bool success = true;


    for (uint i = 0; i < 200; i++) {
        printf(">>>>>>>>>>>>>>>>>>>>>test num: %d\n", i);
        memset(buffer, 0, 1024);
        char *block = generate_random_block(1024, i);
        if(!test_sndrecv(io, role, block, buffer)) {
            printf("Error\n");
            success = false;
        }
        delete [] block;
        printf(">>>>>>>>>>>>>>>>>>>>>test done\n");
    }
    if (success)
        printf("Success\n");
    else
        printf("Error\n");

    delete io;
    delete [] buffer;

    return 0;
}