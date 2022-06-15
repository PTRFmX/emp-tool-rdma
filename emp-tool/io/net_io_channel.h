#ifndef EMP_NETWORK_IO_CHANNEL
#define EMP_NETWORK_IO_CHANNEL

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include "emp-tool/io/io_channel.h"
using std::string;


#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
 
#define VERB_ERR(verb, ret) \
        fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)
 
/* Default parameter values */
#define DEFAULT_PORT "23333"
#define DEFAULT_MSG_LENGTH 1 << 21

namespace emp {

struct context {
    /* User parameters */
    int server;
    char *server_name;
    char *server_port;
    int msg_count;
    int msg_length;
    int msec_delay;
    uint8_t alt_srcport;
    uint16_t alt_dlid;
    uint16_t my_alt_dlid;
    int migrate_after;
 
    /* Resources */
    struct rdma_cm_id *id;
    struct rdma_cm_id *listen_id;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    char *send_buf;
    char *recv_buf;
    pthread_t async_event_thread;
};

int send_id = 0, recv_id = 0;

class NetIO: public IOChannel<NetIO> { public:

	// TCP Socket
	bool is_server;
	int mysocket = -1;
	int consocket = -1;
	FILE * stream = nullptr;
	char * buffer = nullptr;
	bool has_sent = false;
	string addr;
	int port;

	// RDMA 
	int ret, op, i;
	struct context curr_ctx;
	struct ibv_qp_attr qp_attr;

	bool rdma_enabled;

	NetIO(const char * address, int port, bool quiet = false, bool rdma_enabled = false) {

		this->rdma_enabled = rdma_enabled;

		if (rdma_enabled) {
			struct context ctx;
			memset(&ctx, 0, sizeof(ctx));
			memset(&qp_attr, 0, sizeof(qp_attr));

			if (address == nullptr) {
				printf("Enter server mode\n");
				ctx.server = 1;
			} else {
				printf("Enter client mode\n");
				ctx.server = 0;
				ctx.server_name = (char*)address;
			}
			ctx.server_port = DEFAULT_PORT;
			// ctx.msg_count = DEFAULT_MSG_COUNT;
			ctx.msg_length = DEFAULT_MSG_LENGTH;
			// ctx.msec_delay = DEFAULT_MSEC_DELAY;

			if (!ctx.server && !ctx.server_name) {
				printf("server address must be specified for client mode\n");
				exit(1);
			}

			ret = getaddrinfo_and_create_ep(&ctx);
			if (ret) VERB_ERR("getaddrinfo_and_create_ep", ret);
		
			if (ctx.server) {
				ret = get_connect_request(&ctx);
				if (ret) VERB_ERR("get_connect_request", ret);
			}

			ret = reg_mem(&ctx);
			if (ret) VERB_ERR("reg_mem", ret);

			printf("Successfully registered memory\n");
		
			ret = establish_connection(&ctx);

			printf("Successfully connected\n");

			this->curr_ctx = ctx;

		} else {
			if (port < 0 || port > 65535) {
				throw std::runtime_error("Invalid port number!");
			}

			this->port = port;
			is_server = (address == nullptr);
			if (address == nullptr) {
				struct sockaddr_in dest;
				struct sockaddr_in serv;
				socklen_t socksize = sizeof(struct sockaddr_in);
				memset(&serv, 0, sizeof(serv));
				serv.sin_family = AF_INET;
				serv.sin_addr.s_addr = htonl(INADDR_ANY); /* set our address to any interface */
				serv.sin_port = htons(port);           /* set the server port number */
				mysocket = socket(AF_INET, SOCK_STREAM, 0);
				int reuse = 1;
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
			else {
				addr = string(address);

				struct sockaddr_in dest;
				memset(&dest, 0, sizeof(dest));
				dest.sin_family = AF_INET;
				dest.sin_addr.s_addr = inet_addr(address);
				dest.sin_port = htons(port);

				while(1) {
					consocket = socket(AF_INET, SOCK_STREAM, 0);

					if (connect(consocket, (struct sockaddr *)&dest, sizeof(struct sockaddr)) == 0) {
						break;
					}

					close(consocket);
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
		
	}

		
	int reg_mem(struct context *ctx)
	{
		ctx->send_buf = (char *) malloc(ctx->msg_length);
		memset(ctx->send_buf, 0, ctx->msg_length);
	
		ctx->recv_buf = (char *) malloc(ctx->msg_length);
		memset(ctx->recv_buf, 0, ctx->msg_length);
	
		ctx->send_mr = rdma_reg_msgs(ctx->id, ctx->send_buf, ctx->msg_length);
		if (!ctx->send_mr) {
			VERB_ERR("rdma_reg_msgs", -1);
			return -1;
		}
	
		ctx->recv_mr = rdma_reg_msgs(ctx->id, ctx->recv_buf, ctx->msg_length);
		if (!ctx->recv_mr) {
			VERB_ERR("rdma_reg_msgs", -1);
			return -1;
		}
	
		return 0;
	}

	int getaddrinfo_and_create_ep(struct context *ctx)
	{
		int ret;
		struct rdma_addrinfo *rai, hints;
		struct ibv_qp_init_attr qp_init_attr;
	
		memset(&hints, 0, sizeof (hints));
		hints.ai_port_space = RDMA_PS_IB;
		if (ctx->server == 1)
			hints.ai_flags = RAI_PASSIVE; /* this makes it a server */
	
		printf("rdma_getaddrinfo\n");
		ret = rdma_getaddrinfo(ctx->server_name, ctx->server_port, &hints, &rai);
		if (ret) {
			VERB_ERR("rdma_getaddrinfo", ret);
			return ret;
		}
	
		memset(&qp_init_attr, 0, sizeof (qp_init_attr));
	
		qp_init_attr.cap.max_send_wr = 1;
		qp_init_attr.cap.max_recv_wr = 1;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
	
		printf("rdma_create_ep\n");
		ret = rdma_create_ep(&ctx->id, rai, NULL, &qp_init_attr);
		if (ret) {
			VERB_ERR("rdma_create_ep", ret);
			return ret;
		}
	
		rdma_freeaddrinfo(rai);
	
		return 0;
	}

	int get_connect_request(struct context *ctx)
	{
		int ret;
	
		printf("rdma_listen\n");
		ret = rdma_listen(ctx->id, 10);
		if (ret) {
			VERB_ERR("rdma_listen", ret);
			return ret;
		}
	
		ctx->listen_id = ctx->id;
	
		printf("rdma_get_request\n");
		ret = rdma_get_request(ctx->listen_id, &ctx->id);
		if (ret) {
			VERB_ERR("rdma_get_request", ret);
			return ret;
		}
	
		if (ctx->id->event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
			printf("unexpected event: %s",
				rdma_event_str(ctx->id->event->event));
			return ret;
		}
		return 0;
	}

	int establish_connection(struct context *ctx)
	{
		int ret;
		uint16_t private_data;
		struct rdma_conn_param conn_param;
	
		/* post a receive to catch the first send */
		ret = rdma_post_recv(ctx->id, NULL, ctx->recv_buf, ctx->msg_length,
							ctx->recv_mr);
		if (ret) {
			VERB_ERR("rdma_post_recv", ret);
			return ret;
		}
	
		/* send the dlid for the alternate port in the private data */
		// private_data = htons(ctx->my_alt_dlid);
	
		memset(&conn_param, 0, sizeof(conn_param));
		// conn_param.private_data_len = sizeof (int);
		// conn_param.private_data = &private_data;
		conn_param.responder_resources = 2;
		conn_param.initiator_depth = 2;
		conn_param.retry_count = 5;
		conn_param.rnr_retry_count = 5;
	
		if (ctx->server) {
			printf("rdma_accept\n");
			ret = rdma_accept(ctx->id, &conn_param);
			if (ret) {
				VERB_ERR("rdma_accept", ret);
				return ret;
			}
		}
		else {
			printf("rdma_connect\n");
			ret = rdma_connect(ctx->id, &conn_param);
			if (ret) {
				VERB_ERR("rdma_connect", ret);
				return ret;
			}
			if (ctx->id->event->event != RDMA_CM_EVENT_ESTABLISHED) {
				printf("unexpected event: %s",
					rdma_event_str(ctx->id->event->event));
				return -1;
			}
		}
		return 0;
	}

	int send_msg(struct context *ctx) {
		int ret;
		struct ibv_wc wc;
		auto t0 = clock_start();
		ret = rdma_post_send(ctx->id, NULL, ctx->send_buf, ctx->msg_length,
							ctx->send_mr, IBV_SEND_SIGNALED);
		// printf("RDMA SEND time: %llu\n", time_from(t0));
		if (ret) {
			VERB_ERR("rdma_send_recv", ret);
			return ret;
		}
		auto t1 = clock_start();
		ret = rdma_get_send_comp(ctx->id, &wc);
		// printf("RDMA POLL SEND CQ time: %llu\n", time_from(t1));
		if (ret < 0) {
			VERB_ERR("rdma_get_send_comp", ret);
			return ret;
		}	
		return 0;
	}

	int recv_msg(struct context *ctx)
	{
		int ret;
		struct ibv_wc wc;
		
		auto t0 = clock_start();
		ret = rdma_get_recv_comp(ctx->id, &wc);
		// printf("RDMA POLL RECV CQ time: %llu\n", time_from(t0));
		if (ret < 0) {
			VERB_ERR("rdma_get_recv_comp", ret);
			return ret;
		}

		auto t1 = clock_start();
		ret = rdma_post_recv(ctx->id, NULL, ctx->recv_buf, ctx->msg_length,
							ctx->recv_mr);
		// printf("RDMA RECV time: %llu\n", time_from(t1));
		if (ret) {
			VERB_ERR("rdma_post_recv", ret);
			return ret;
		}
	
		return 0;
	}

	~NetIO(){
		if (this->rdma_enabled) {
			rdma_disconnect((this->curr_ctx).id);
		} else {
			flush();
			fclose(stream);
			delete[] buffer;
		}
	}

	void sync() {
		if (this->rdma_enabled) {
			printf("calling sync\n");		
		} else {
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
	}

	void set_nodelay() {
		const int one = 1;
		setsockopt(consocket, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	}

	void set_delay() {
		const int zero = 0;
		setsockopt(consocket, IPPROTO_TCP, TCP_NODELAY, &zero, sizeof(zero));
	}

	void flush() {
		if (this->rdma_enabled) {
			printf("calling flush\n");
		} else {
			fflush(stream);
		}
	}

	void send_data_internal(const void * data, size_t len) {
		if (this->rdma_enabled) {
			memcpy((this->curr_ctx).send_buf, (char*)data, len);
			if (send_msg(&(this->curr_ctx))) {
				VERB_ERR("send_data_internal -> send_msg", -1);
			}
			memset((this->curr_ctx).send_buf, 0, (this->curr_ctx).msg_length);
		} else {
			size_t sent = 0;
			while(sent < len) {
				size_t res = fwrite(sent + (char*)data, 1, len - sent, stream);
				if (res > 0)
					sent+=res;
				else
					error("net_send_data\n");
			}
			has_sent = true;
		}
		// printf("sending data internal with len=%d, send_id=%d\n", len, send_id++);
	}

	void recv_data_internal(void * data, size_t len) {
		// printf("recving data internal with len=%d, recv_id=%d\n", len, recv_id++);
		if (this->rdma_enabled) {
			if (recv_msg(&(this->curr_ctx))) {
				VERB_ERR("recv_data_internal -> recv_msg", -1);
			}
			memcpy((char*)data, (this->curr_ctx).recv_buf, len);
			memset((this->curr_ctx).recv_buf, 0, (this->curr_ctx).msg_length);
		} else {
			if(has_sent)
				fflush(stream);
			has_sent = false;
			size_t sent = 0;
			while(sent < len) {
				size_t res = fread(sent + (char*)data, 1, len - sent, stream);
				if (res > 0)
					sent += res;
				else
					error("net_recv_data\n");
			}
		}
	}
};

}

#endif  //NETWORK_IO_CHANNEL
