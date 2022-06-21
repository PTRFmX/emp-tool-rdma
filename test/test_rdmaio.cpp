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
    printf("generate at 0: %d\n", block[0]);
    return block;
}

bool test_sndrecv(RDMAIO *io, uint32_t role, char *block, char *buffer, uint iter) {
    if ((iter % 4 == 0 && role == 0) || (iter % 4 == 1 && role == 0) || (iter % 4 == 2 && role == 1) || (iter % 4 == 3 && role == 1))
        io->send_data(block, 1024); // client send data
    else
        io->recv_data((void *)buffer, 1024); // server recv data
    io->sync();
    if (role == (iter % 4 < 2)) { // compare received data
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


void simulate_recv_pt(RDMAIO *io) {

    uint32_t block_size;
    io->recv_data(&block_size, 4);
    io->flush();
    char *random_block = generate_random_block(block_size, 0);

    io->recv_data(random_block, block_size);
    io->flush();
    delete [] random_block;
}

void simulate_send_pt(RDMAIO *io, uint32_t length) {
    io->send_data(&length, 4);
    io->flush();
    char *random_block = generate_random_block(length, 0);
    io->send_data(random_block, length);
    io->flush();

    delete [] random_block;
}

void simulate_ot(RDMAIO *io, uint32_t role, uint32_t length) {
    if (role == 0) {
        simulate_send_pt(io, 128);
        io->flush();
        printf("current 3 flushes\n");
        for (uint i = 0; i < length; i++) {
            printf("recv_pt: %d\n", i);
            simulate_recv_pt(io);
        }
        printf("current 256 + 3 = 259 flushes\n");
        for (uint i = 0; i < length; i++) {
            simulate_send_pt(io, 128);
        }
        io->flush();
        printf("current 513 + 3 = 516 flushes\n");
        char *rand_block = generate_random_block(32, 0);
        for (uint i = 0; i < length; i++) {
            io->send_data(rand_block, 32);
        }
        io->flush();
        printf("current 516 + 128 = 624 flushes\n");
    } else {
        simulate_recv_pt(io);
        io->flush();
        printf("current 3 flushes\n");

        for (uint i = 0; i < length; i++) {
            printf("send_pt: %d\n", i);
            simulate_send_pt(io, 128);
        }
        printf("current 256 + 3 = 259 flushes\n");
        for (uint i = 0; i < length; i++) {
            simulate_recv_pt(io);
        }

        io->flush();
        printf("current 513 + 3 = 516 flushes\n");

        char *rand_block = generate_random_block(32, 0);
        for (uint i = 0; i < length; i++) {
            io->recv_data(rand_block, 32);
        }
        io->flush();
        printf("current 516 + 128 = 624 flushes\n");
    }
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


    for (uint i = 0; i < 1000; i++) {
        printf(">>>>>>>>>>>>>>>>>>>>>test num: %d\n", i);
        memset(buffer, 0, 1024);
        char *block = generate_random_block(1024, i);
        if(!test_sndrecv(io, role, block, buffer, i)) {
            printf("Error\n");
            success = false;
        }
        io->flush();
        delete [] block;
        printf(">>>>>>>>>>>>>>>>>>>>>test done\n");
    }
    if (success)
        printf("Success\n");
    else
        printf("Error\n");

    // simulate_ot(io, role, 128);

    delete io;
    delete [] buffer;

    // for (uint i = 0; i < 200; i++) {
    //     printf("flush %d\n", i);
    //     io->flush();
    // }

    return 0;
}