#ifndef EMP_NETWORK_IO_CHANNEL
#define EMP_NETWORK_IO_CHANNEL

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include "emp-tool/io/io_channel.h"
using std::string;

#include <rdma/rsocket.h>
#include <rdma/rdma_cma.h>

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace emp {

uint64_t wr_id = 0;

class NetIO: public IOChannel<NetIO> { public:
	bool is_server;
	int mysocket = -1;
	int consocket = -1;
	FILE * stream = nullptr;
	char * buffer = nullptr;
	bool has_sent = false;
	string addr;
	int port;
	bool rs;
	NetIO(const char * address, int port, bool quiet = false, bool rs = false) {
		if (port < 0 || port > 65535) {
			throw std::runtime_error("Invalid port number!");
		}

		if (rs) {
			printf("Using RDMA Socket\n");
		}
		else {
			printf("Using TCP Socket\n");
		}

		this->port = port;
		this->rs = rs;

		is_server = (address == nullptr);
		if (address == nullptr) {
			struct sockaddr_in dest;
			struct sockaddr_in serv;
			socklen_t socksize = sizeof(struct sockaddr_in);
			memset(&serv, 0, sizeof(serv));
			serv.sin_family = AF_INET;
			serv.sin_addr.s_addr = htonl(INADDR_ANY); /* set our address to any interface */
			serv.sin_port = htons(port);           /* set the server port number */
			
			int reuse = 1;
			if (rs) {
				mysocket = rsocket(AF_INET, SOCK_STREAM, 0);
				rsetsockopt(mysocket, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse));
				
				if(rbind(mysocket, (struct sockaddr *)&serv, sizeof(struct sockaddr)) < 0) {
					perror("error: bind");
					exit(1);
				}
				if(rlisten(mysocket, 1) < 0) {
					perror("error: listen");
					exit(1);
				}
				consocket = raccept(mysocket, (struct sockaddr *)&dest, &socksize);
				rclose(mysocket);
			}
			else {
				mysocket = socket(AF_INET, SOCK_STREAM, 0);
				setsockopt(mysocket, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse));
				
				if(bind(mysocket, (struct sockaddr *)&serv, sizeof(struct sockaddr)) < 0) {
					perror("error: bind");
					exit(1);
				}
				if(listen(mysocket, 1) < 0) {
					perror("error: listen");
					exit(1);
				}
				consocket = accept(mysocket, (struct sockaddr *)&dest, &socksize);
				close(mysocket);
			}
		}
		else {
			addr = string(address);

			struct sockaddr_in dest;
			memset(&dest, 0, sizeof(dest));
			dest.sin_family = AF_INET;
			dest.sin_addr.s_addr = inet_addr(address);
			dest.sin_port = htons(port);
			while(1) {
				if (rs) {
					consocket = rsocket(AF_INET, SOCK_STREAM, 0);
					if (rconnect(consocket, (struct sockaddr *)&dest, sizeof(struct sockaddr)) == 0) {
						break;
					}
					rclose(consocket);
				}
				else {
					consocket = socket(AF_INET, SOCK_STREAM, 0);
					if (connect(consocket, (struct sockaddr *)&dest, sizeof(struct sockaddr)) == 0) {
						break;
					}
					close(consocket);
				}
				usleep(1000);
			}
		}
		set_nodelay();
		stream = fdopen(consocket, "wb+");
		buffer = new char[NETWORK_BUFFER_SIZE];
		memset(buffer, 0, NETWORK_BUFFER_SIZE);
		setvbuf(stream, buffer, _IOFBF, NETWORK_BUFFER_SIZE);
		if(!quiet)
			std::cout << "connected\n";
	}

	void sync() {
		int tmp = 0;
		if(is_server) {
			send_data_internal(&tmp, 1);
			recv_data_internal(&tmp, 1);
		} else {
			recv_data_internal(&tmp, 1);
			send_data_internal(&tmp, 1);
			flush();
		}
	}

	~NetIO(){
		flush();
		fclose(stream);
		delete[] buffer;
	}

	void set_nodelay() {
		const int one = 1;
		if (this->rs) {
			rsetsockopt(consocket,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
		}
		else {
			setsockopt(consocket,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
		}
	}

	void set_delay() {
		const int zero = 0;
		if (this->rs) {
			rsetsockopt(consocket,IPPROTO_TCP,TCP_NODELAY,&zero,sizeof(zero));
		}
		else {
			setsockopt(consocket,IPPROTO_TCP,TCP_NODELAY,&zero,sizeof(zero));
		}
	}

	void flush() {
		// fflush(stream);
	}

	// void printData(void* data, size_t len) {
	// 	for (size_t i = 0; i < len; i++) {
	// 		printf("0x%x ", *(i + (char*)data));
	// 	}
	// 	printf("\n");
	// }

	void send_data_internal(const void * data, size_t len) {
		size_t sent = 0;
		while(sent < len) {
			// size_t res = this->rs ? rwrite(consocket, sent + (char*)data,  len - sent) : fwrite(sent + (char*)data, 1, len - sent, stream);
			size_t res = this->rs ? rwrite(consocket, sent + (char*)data, len - sent) : send(consocket, sent + (char*)data, len - sent, 0);
			if (res > 0)
				sent += res;
			else
				error("net_send_data\n");
		}
		// has_sent = true;
	}

	void recv_data_internal(void  * data, size_t len) {
		// if(has_sent) {
		// 	fflush(stream);
		// }
		// has_sent = false;
		size_t sent = 0;
		while(sent < len) {
			// size_t res = this->rs ? rread(consocket, sent + (char*)data,  len - sent) : fread(sent + (char*)data, 1, len - sent, stream);
			size_t res = this->rs ? rread(consocket, sent + (char*)data, len - sent) : recv(consocket, sent + (char*)data, len - sent, 0);
			if (res > 0)
				sent += res;
			else
				error("net_recv_data\n");
		}
	}
};

}

#endif  //NETWORK_IO_CHANNEL
