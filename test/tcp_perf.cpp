#include "emp-tool/io/net_io_channel.h"
#include <iostream>
#include <string>
#include <cstdio>
#include <chrono>

using namespace emp;
#define BLOCK_SIZE (1 << 20)


char* generate_random_block(size_t size, uint32_t seed) {
    srand(seed);
    char* block = new char[size];
    for (size_t i = 0; i < size; ++i) {
        block[i] = rand() % 256;
    }
    return block;
}


int main(int argc, char **argv) {
    if (argc < 4) {
        std::cout << "Usage: ./test_netio <role> <host> <port>" << std::endl;
        return -1;
    }
    uint role = atoi(argv[1]);
    std::string host = argv[2];
    uint port = atoi(argv[3]);

    NetIO *io = new NetIO(role == 0? nullptr: host.c_str(), port);

    char *buffer = new char[BLOCK_SIZE];
    bool success = true;


    char *block = generate_random_block(BLOCK_SIZE, 2);

    auto start = std::chrono::high_resolution_clock::now();
    if (role == 0) {
        io->send_data(block, BLOCK_SIZE);
        io->flush();
    } else {
        io->recv_data(buffer, BLOCK_SIZE);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> fp_us = end - start;
    double duration_us = fp_us.count();
    std::cout << "elapsed time " << duration_us << std::endl;

    if (success)
        printf("Success\n");
    else
        printf("Error\n");

    delete io;
    delete [] buffer;

    return 0;
}