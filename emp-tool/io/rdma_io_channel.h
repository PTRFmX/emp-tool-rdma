#ifndef EMP_RDMA_IO_CHANNEL_RDMA_H
#define EMP_RDMA_IO_CHANNEL_RDMA_H

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include "emp-tool/io/io_channel.h"
using std::string;
#include <rdma/rdma_cma.h>
#include <netdb.h>
#include <thread>
#include <utility>

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>

#include <errno.h>

#define TEST_NZ(x)                                      \
  do                                                    \
  {                                                     \
    if ((x))                                            \
    {                                                   \
      printf("errno: %s\n", strerror(errno));           \
      die("error: " #x " failed (returned non-zero)."); \
    }                                                   \
  } while (0)
#define TEST_Z(x)                                        \
  do                                                     \
  {                                                      \
    if (!(x))                                            \
    {                                                    \
      printf("errno: %s\n", strerror(errno));            \
      die("error: " #x " failed (returned zero/null)."); \
    }                                                    \
  } while (0)

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  getchar();
  exit(EXIT_FAILURE);
}

const int TIMEOUT_IN_MS = 500; /* ms */
#define RDMA_BUFFER_SIZE (1UL << 26)
#define BATCH_THREASHOLD 256

#define MAX_INLINE_DATA 128

#define TX_BUFFER_DEP 1024
#define RX_BUFFER_DEP 2048

namespace emp
{
  struct ibv_send_wr *bad_wr = NULL;
  using offset_and_size = std::pair<uint64_t, uint64_t>;
  using WriteDescriptor = std::vector<offset_and_size>;

  WriteDescriptor add_wd(WriteDescriptor &wd1, WriteDescriptor &wd2) {
    WriteDescriptor res(2);

    if (wd1[0].first + wd1[0].second == wd2[0].first) {
      res[0] = {wd1[0].first, wd1[0].second + wd2[0].second};
      res[1] = {0, wd1[1].second + wd2[1].second};
    } else { // wrap around
      res[0] = wd1[0];
      res[1] = wd1[1];
      res[1].second += wd2[0].second;
      if (wd2[1].second > 0) {
        die("detect wd error");
      }
    }

    return res;
  }

  size_t pending_write(WriteDescriptor &wd) {
    return wd[0].second + wd[1].second;
  }
    
  class StreamBuffer
  {
  public:
    
    StreamBuffer()
    {
      buffer_len = RDMA_BUFFER_SIZE;
    }

    ~StreamBuffer()
    {

    }

    WriteDescriptor write(const void *data, size_t len)
    {
      // copy data to ppos; update ppos

      WriteDescriptor res(2);

      // check available buffer len
      if (available_len() < len) {
        printf("buffer space not enough\n");
        die("exited");
      }

      if (ppos + len > buffer_len) { // two blocks
        uint64_t first_block_size = buffer_len - ppos;
        uint64_t second_block_size = len - first_block_size;

        res[0] = {ppos, first_block_size};
        res[1] = {0, second_block_size};

        memcpy((char *)buffer + ppos, data, first_block_size);
        memcpy((char *)buffer, data + first_block_size, second_block_size);
        ppos = second_block_size;
      } else { // one block
        res[0] = {ppos, len};
        res[1] = {0, 0};

        memcpy((char *)buffer + ppos, data, len);
        ppos += len;
        ppos = ppos % buffer_len; // in case ppos is after the last one.
      }

      write_size_total += len;
      return res;
    }

    size_t read(void *dst, size_t len)
    {
      if (data_len() < len) {
        printf("error, cannot read that much data, available len: %ld\n", data_len());
        die("exited");
      }

      if (gpos + len > buffer_len) {
        uint64_t first_block_size = buffer_len - gpos;
        uint64_t second_block_size = len - first_block_size;

        memcpy(dst, (char *)buffer + gpos, first_block_size);
        memcpy(dst + first_block_size, buffer, second_block_size);
        gpos = second_block_size;
      } else {
        memcpy(dst, (char *)buffer + gpos, len);
        gpos += len;
        gpos = gpos % buffer_len; // in case ppos is after the last one.
      }
      read_size_total += len;
      return len;
    }

    // call from receiver return the size of data can be read.
    uint64_t data_len()
    {
      if (write_size_total < read_size_total)
      {
        printf("error, write is less than read\n");
        die("exited");
      }
      // printf("current data_len: %d\n", write_size_total - read_size_total);
      return write_size_total - read_size_total;
    }

    // call from sender to check the available length of buffer
    uint64_t available_len()
    {
      if (write_size_total - *remote_read_size_total_ptr > buffer_len)
      {
        printf("error: we have writen too much data to remote\n");
        printf("write size total: %d, remote_read_size_total: %d\n", write_size_total, *remote_read_size_total_ptr);
        die("exited");
      }
      return buffer_len - ( write_size_total - *remote_read_size_total_ptr );
    }

    char *get_addr(uint64_t pos) 
    {
      return (char *)buffer + pos;
    }


    uint64_t gpos = 0;             // read pointer
    uint64_t ppos = 0;             // write pointer
    volatile uint64_t write_size_total = 0; // total data writen, in bytes, need to write to the other side
    volatile uint64_t read_size_total = 0;  // total data read, in bytes
    volatile uint64_t remote_read_size_total = 0;
    volatile uint64_t *remote_read_size_total_ptr = nullptr;

    char buffer[RDMA_BUFFER_SIZE];
    size_t buffer_len;
  };

  enum msg_type
  {
    MSG_MR,
    MSG_DONE
  };

  struct message
  {
    msg_type type;
    union
    {
      struct ibv_mr mr;
    } data;
    uint64_t size;
  };

  enum s_state
  {
    SS_INIT,
    SS_MR_SENT,
    SS_WAIT_MSG_DATA,
    SS_DONE_SENT
  };

  enum r_state
  {
    RS_INIT,
    RS_MR_RECV,
    RS_DONE_RECV
  };

  struct conn_context
  {

    struct rdma_cm_id *id;
    struct ibv_qp *qp;

    int connected;

    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;
    struct ibv_mr *rdma_local_mr;
    struct ibv_mr *rdma_remote_mr;

    struct ibv_mr peer_mr;

    message *recv_msg;
    message *send_msg;

    void *rdma_local_buffer;
    void *rdma_remote_buffer;

    s_state send_state;
    r_state recv_state;
  };

  struct context
  {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    struct ibv_comp_channel *comp_channel;

    std::thread *cq_poller_thread;
    std::thread *snd_cq_poller;
    std::thread *rcv_cq_poller;
  };

  class RDMAIO : public IOChannel<RDMAIO>
  {
  public:
    bool is_server;
    string _address;
    int port;

    // party is ready for send & recv
    bool party_ready = false;
    volatile bool party_done = false;
    // party is ready for next send & recv
    bool send_ready = false;
    std::chrono::high_resolution_clock::time_point end;

    // bool initialized = false;

    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *client_conn = NULL;
    struct addrinfo *server_addr;
    struct context *s_ctx = NULL;
    struct conn_context *_conn_context = NULL;

    RDMAIO(const char *address, int port, bool quiet = false)
    {
      is_server = (address == nullptr);
      pending_wd.resize(2);
      pending_wd[0] = {0, 0};
      pending_wd[1] = {0, 0};
      
      if (is_server) {
        struct sockaddr_in sin;
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = htons(port);
        TEST_Z(ec = rdma_create_event_channel());
        TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
        TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&sin));
        TEST_NZ(rdma_listen(listener, 10));
        port = ntohs(rdma_get_src_port(listener));
        printf("listening on port %d.\n", port);
        while (rdma_get_cm_event(ec, &event) == 0) { // init connection
          struct rdma_cm_event event_copy;
          memcpy(&event_copy, event, sizeof(*event));
          rdma_ack_cm_event(event);
          if (on_event_server(&event_copy))
            break;
        }
      } else { // client
        _address = string(address);
        this->port = port;
        std::string port_ss = std::to_string(port);
        TEST_NZ(getaddrinfo(address, port_ss.c_str(), NULL, &server_addr));
        TEST_Z(ec = rdma_create_event_channel());
        TEST_NZ(rdma_create_id(ec, &client_conn, NULL, RDMA_PS_TCP));
        // client resolve addr to server; this is the first step for establishing connection
        TEST_NZ(rdma_resolve_addr(client_conn, NULL, server_addr->ai_addr, TIMEOUT_IN_MS));

        freeaddrinfo(server_addr);
        while (rdma_get_cm_event(ec, &event) == 0) {
          struct rdma_cm_event event_copy;
          memcpy(&event_copy, event, sizeof(*event));
          rdma_ack_cm_event(event);
          // event driven connection function
          if (on_event_client(&event_copy))
            break;
        }
      }

      init_sndrecv();
      
      if (!quiet)
        std::cout << "connection establishment finished\n";
    
    }

    ~RDMAIO()
    { 
      // remember to sync before destructing
      sync();

      if (is_server) {
        if (!_conn_context) // no client connected.
          rdma_destroy_id(listener);
        else {
          while (rdma_get_cm_event(ec, &event) == 0)
          {
            struct rdma_cm_event event_copy;
            memcpy(&event_copy, event, sizeof(*event));
            rdma_ack_cm_event(event);
            if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {
              party_done = true;
              on_disconnect_server((&event_copy)->id);
              break;
            } else{
              printf("not disconnect event\n");
            }
          }
        }
      } else { // client
        // send_message(_conn_context, MSG_DONE);
        party_done = true;
        rdma_disconnect(_conn_context->id);
        while (rdma_get_cm_event(ec, &event) == 0) {
          struct rdma_cm_event event_copy;
          memcpy(&event_copy, event, sizeof(*event));
          rdma_ack_cm_event(event);
          if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {
            on_disconnect_client((&event_copy)->id);
            break;
          } else {
            printf("not disconnect event\n");
          }
        }
      }
      rdma_destroy_event_channel(ec);
    }

    int on_event_client(struct rdma_cm_event *event)
    {
      int r = 0;

      if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
        r = on_addr_resolved(event->id);
      else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
        r = on_route_resolved(event->id);
      else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        r = on_connection_client(event->id);
      else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
        r = on_disconnect_client(event->id);
      else
      {
        fprintf(stderr, "on_event: %d\n", event->event);
        die("on_event: unknown event.");
      }

      return r;
    }

    int on_addr_resolved(struct rdma_cm_id *id)
    {
      // call on client
      printf("address resolved.\n");

      build_connection(id);
      TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

      return 0;
    }

    int on_route_resolved(struct rdma_cm_id *id)
    {
      struct rdma_conn_param cm_params;

      printf("route resolved.\n");
      build_params(&cm_params);
      TEST_NZ(rdma_connect(id, &cm_params));
      return 0;
    }

    void send_mr(void *context)
    {
      struct conn_context *conn = (struct conn_context *)context;

      for (uint i = 0; i < 2; i++)
        memcpy(&conn->send_msg[i].data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));
      send_message(conn, MSG_MR);
    }

    int on_disconnect_client(struct rdma_cm_id *id)
    {
      printf("disconnected.\n");

      destroy_connection(id->context);
      return 1; /* exit event loop */
    }

    int on_disconnect_server(struct rdma_cm_id *id)
    {
      printf("peer disconnected.\n");

      destroy_connection(id->context);
      return 1; // break loop
    }

    uint rcnt = 0;

    void init_sndrecv()
    {
      poll_cq_connect(nullptr);
      // s_ctx->cq_poller_thread = new std::thread([this]()
                                                // { poll_cq_flush(_conn_context); });
      s_ctx->snd_cq_poller = new std::thread([this](){ poll_snd_cq(_conn_context); });
      s_ctx->rcv_cq_poller = new std::thread([this](){ poll_recv_cq(_conn_context); });
    }

    void poll_cq_connect(void *ctx)
    {
      struct ibv_cq *cq;
      struct ibv_wc wc;

      while (1) {
        if (!cq_wc_queue.empty()) {
          die("I assume cq_wc_queue is empty");
        }
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));

        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));

        int num_comp = ibv_poll_cq(cq, 1, &wc);
        while (num_comp && !party_ready) {
          // printf("num_comp: %d\n", num_comp);
          on_completion(&wc, is_server);
          // printf("poll cq again, party ready: %d\n", party_ready);
          num_comp = ibv_poll_cq(cq, 1, &wc);
          // printf("num_comp: %d\n", num_comp);
        }

        if (num_comp) {
          cq_wc_queue.push(wc);
          while (ibv_poll_cq(cq, 1, &wc)) {
            cq_wc_queue.push(wc);
          }
        }

        if (party_ready) {
          printf("party ready\n");
          break;
        }
      }
    }

    void destroy_connection(void *context)
    {
      struct conn_context *conn = (struct conn_context *)context;
      rdma_destroy_qp(conn->id);

      ibv_dereg_mr(conn->send_mr);
      ibv_dereg_mr(conn->recv_mr);
      ibv_dereg_mr(conn->rdma_local_mr);
      ibv_dereg_mr(conn->rdma_remote_mr);

      free(conn->send_msg);
      free(conn->recv_msg);
      free(conn->rdma_local_buffer);
      free(conn->rdma_remote_buffer);

      rdma_destroy_id(conn->id);

      delete conn;
    }


    int on_event_server(struct rdma_cm_event *event)
    {
      int r = 0;

      if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        r = on_connect_request(event->id);
      else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        r = on_connection_server(event->id);
      else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
        r = on_disconnect_server(event->id);
      else
        die("on_event: unknown event.");

      return r;
    }

    int on_connection_server(struct rdma_cm_id *id)
    {
      _conn_context = (struct conn_context *)id->context;
      _conn_context->connected = 1;

      return 1; // break loop
    }

    int on_connection_client(struct rdma_cm_id *id)
    {
      _conn_context = (struct conn_context *)id->context;
      _conn_context->connected = 1;
      printf("send mr\n");
      send_mr(id->context);
      return 1;
    }

    bool has_sent = false;
    WriteDescriptor pending_wd;

    void send_data_internal(const void *data, size_t len, bool flush = false)
    {
      // printf("send: %d\n", len);
      StreamBuffer *buf = (StreamBuffer *)_conn_context->rdma_local_buffer;
      WriteDescriptor new_wd;
      
      if (data != NULL && len != 0) {
        new_wd = buf->write(data, len);
        // remember to put new_wd on the second argument
        pending_wd = add_wd(pending_wd, new_wd);
        // printf("add:(%d, %d), (%d, %d), current pending wd: \n", new_wd[0].first, new_wd[0].second, new_wd[1].first, new_wd[1].second);
        // printf("(%d, %d), (%d, %d)\n", pending_wd[0].first, pending_wd[0].second, pending_wd[1].first, pending_wd[1].second);
        has_sent = true;
      }

      if (pending_write(pending_wd) >= BATCH_THREASHOLD || flush) {
        post_send_data(pending_wd, _conn_context->id);
        if (pending_wd[1].second > 0) {
          pending_wd[0] = {pending_wd[1].first + pending_wd[1].second, 0};
          pending_wd[1] = {0, 0};
        } else {
          pending_wd[0] = {pending_wd[0].first + pending_wd[0].second, 0};
          pending_wd[1] = {0, 0};
        }
        has_sent = false;
      }
    }

    uint64_t posted_rdma_write = 0;

    volatile uint64_t completed_send_wr = 0;
    uint64_t post_send_data_time = 0;
    uint64_t wait_remote_read_time = 0;
    uint64_t wait_tx_buffer_time = 0;
    uint64_t set_wr_time = 0;
    uint64_t ibv_post_send_time = 0;

    void post_send_data(WriteDescriptor &wds, struct rdma_cm_id *id)
    {
      // printf("post send data\n");
      // auto start = clock_start();
      struct conn_context *conn = (struct conn_context *)id->context;
      struct ibv_send_wr wr[2], wr_update_write, *last_send_wr;
      struct ibv_sge sge[3];

      StreamBuffer *remote_sbf = (StreamBuffer *)conn->peer_mr.addr;
      StreamBuffer *local_sbf = (StreamBuffer *)conn->rdma_local_buffer;

      memset(wr, 0, sizeof(wr));
      memset(&wr_update_write, 0, sizeof(wr_update_write));

      for (int i = 0; i < wds.size(); i++) {
        if (i >= 2) {
          printf("error: too much write descriptor\n");
          die("failed");
        }
        if (wds[i].second > 0) {
          wr[i].wr_id = (uintptr_t)conn;
          wr[i].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
          wr[i].imm_data = wds[i].second;
          wr[i].sg_list = &sge[i];
          wr[i].num_sge = 1;
          wr[i].send_flags = IBV_SEND_SIGNALED;
          wr[i].wr.rdma.remote_addr = (uintptr_t)(remote_sbf->get_addr(wds[i].first));
          wr[i].wr.rdma.rkey = conn->peer_mr.rkey;
          
          sge[i].addr = (uintptr_t)(local_sbf->get_addr(wds[i].first));
          sge[i].length = wds[i].second;
          if (wds[i].second <= MAX_INLINE_DATA)
            wr[i].send_flags = wr[i].send_flags | IBV_SEND_INLINE;
          sge[i].lkey = conn->rdma_local_mr->lkey;
          posted_rdma_write += 1;
          last_send_wr = &wr[i];

        }
        if (i > 0 && wds[i].second > 0) {
          wr[i - 1].next = &wr[i];
        }
      }


      // check wether we can write to the remote side without overwriting unread data
      size_t len = wds[0].second + wds[1].second;
      while(local_sbf->available_len() < len)
        printf("pending avilable len\n");

      // printf("posted_rdma_write: %d\n", posted_rdma_write);
      while(posted_rdma_write - num_completed_write >= TX_BUFFER_DEP) {
        ;
      }

      auto post_res = ibv_post_send(conn->qp, &wr[0], &bad_wr);
      if (post_res != 0) {
        printf("ibv_post_send returned: %d\n", post_res);
        printf("errno message: %s\n", strerror(errno));
      }
      
    }

    void reset_time_counter() {
      post_send_data_time = 0;
      wait_remote_read_time = 0;
      wait_tx_buffer_time = 0;
      set_wr_time = 0;
      ibv_post_send_time = 0;
    }

    void print_time() {
      printf("post_send_data_time: %ld\n", post_send_data_time);
      printf("wait remote_read_time: %ld\n", wait_remote_read_time);
      printf("wait_tx_buffer_time: %ld\n", wait_tx_buffer_time);
      printf("set_wr_time: %ld\n", set_wr_time);
      printf("ibv_post_send_time: %ld\n", ibv_post_send_time);
    }

    void dump_data()
    {
      printf("dump remote rdma buffer\n");
      StreamBuffer *remote_sbuf = (StreamBuffer *)_conn_context->rdma_remote_buffer;
      StreamBuffer *local_sbuf = (StreamBuffer *)_conn_context->rdma_local_buffer;
      printf("remote sbuf: \n");
      printf("%s\n", (char *)remote_sbuf->buffer);
      printf("write_len: %d\n", remote_sbuf->write_size_total);
      printf("read_len: %d\n", remote_sbuf->read_size_total);
      if (remote_sbuf->remote_read_size_total_ptr != nullptr)
        printf("remote read_size_total: %d\n", *(remote_sbuf->remote_read_size_total_ptr));

      printf("local sbuf: \n");
      printf("%s\n", (char *)local_sbuf->buffer);
      printf("write_len: %d\n", local_sbuf->write_size_total);
      printf("read_len: %d\n", local_sbuf->read_size_total);
      if (local_sbuf->remote_read_size_total_ptr != nullptr)
        printf("remote read_size_total: %d\n", *(local_sbuf->remote_read_size_total_ptr));
      
    }

    void post_send_read_len()
    {
      struct conn_context *conn = _conn_context;
      struct ibv_send_wr wr;
      struct ibv_sge sge;

      StreamBuffer *remote_sbf = (StreamBuffer *)conn->peer_mr.addr;
      StreamBuffer *local_sbf = (StreamBuffer *)conn->rdma_remote_buffer;

      memset(&wr, 0, sizeof(wr));

      wr.wr_id = (uintptr_t)conn;
      wr.opcode = IBV_WR_RDMA_WRITE;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
      wr.wr.rdma.remote_addr = (uintptr_t)&(remote_sbf->remote_read_size_total);
      wr.wr.rdma.rkey = conn->peer_mr.rkey;

      sge.addr = (uintptr_t)&(local_sbf->read_size_total);
      sge.length = sizeof(local_sbf->read_size_total);
      sge.lkey = conn->rdma_remote_mr->lkey;

      posted_rdma_write += 1;
      while(posted_rdma_write - num_completed_write >= TX_BUFFER_DEP) {
        ;
      }

      auto post_res = ibv_post_send(conn->qp, &wr, &bad_wr);

      if (post_res != 0) {
        printf("ibv_post_send returned: %d\n", post_res);
        printf("errno message: %s\n", strerror(errno));
      }
    }

    size_t read_accumulated = 0;

    void recv_data_internal(void *data, size_t len)
    {
      // first flush
      if (has_sent)
        flush();
      // blocking
      StreamBuffer *remote_sbf = (StreamBuffer *) _conn_context->rdma_remote_buffer;

      while(remote_sbf->data_len() < len)
        ;
        // printf("waitting for remote data\n");
      
      remote_sbf->read(data, len);
      read_accumulated += len;
      if (read_accumulated >= BATCH_THREASHOLD) {
        post_send_read_len();
        read_accumulated = 0;
      }

    }


    inline uint32_t post_receive_message(struct conn_context *conn, uint num, bool with_message=false)
    {
      // post receive message wait for next instruction

      if (num == 0)
        return 0;

      struct ibv_recv_wr wr, *recv_bad_wr = NULL;
      struct ibv_sge sge;
      memset(&wr, 0, sizeof(wr));
      memset(&sge, 0, sizeof(sge));

      wr.wr_id = (uintptr_t)conn;
      wr.next = NULL;
      if (with_message) {
        wr.sg_list = &sge;
        wr.num_sge = 1;
        sge.addr = (uintptr_t)(conn->recv_msg + (posted_recv_num % 2));
        sge.length = sizeof(struct message);
        sge.lkey = conn->recv_mr->lkey;
      }
      for (uint i = 0; i < num; i++) {
        TEST_NZ(ibv_post_recv(conn->qp, &wr, &recv_bad_wr));
        posted_recv_num += 1;
      }

      return num;
    }

    void on_completion(struct ibv_wc *wc, bool is_server)
    { // it only deals with mr messages and msg done
      struct conn_context *conn = _conn_context;

      if (wc->status != IBV_WC_SUCCESS) {
        printf("error status: %d\n", wc->status);
        die("on_completion: status is not IBV_WC_SUCCESS.");
      }

      if (wc->opcode & IBV_WC_RECV) {
        recved_msg_num += 1;
        if (conn->recv_msg->type == MSG_MR) {
          printf("received MR.\n");
          if (conn->recv_state == RS_INIT)
            conn->recv_state = RS_MR_RECV;
          memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
          post_receive_message(conn, 1);
          if (conn->send_state == SS_INIT) {
            printf("send mr\n");
            send_mr(conn);
          }
        } else if (conn->recv_msg->type == MSG_DONE) {
          die("It should not recv MSG_DONE");
        } else {
          printf("unkown message type: %d\n", conn->recv_msg->type);
          die("on_completion: unknown message type.");
        }
      } else if (wc->opcode == IBV_WC_RDMA_WRITE) {
        // write operation done
        die("should not get rdma_write\n");
      } else if (wc->opcode == IBV_WC_SEND) { 
        // send operation done
        if (conn->send_state == SS_INIT) {
          conn->send_state = SS_MR_SENT;
          send_ready = true;
        } else if (conn->send_state == SS_MR_SENT) {
          die("send operation done after ready should not happen");
        }
      } else { // other opcodes
        printf("unknown opcode: %d\n", wc->opcode);
        die("on_completion: unknown opcode.");
      }
      if (conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV) {
        party_ready = true;
      }
    }

    void flush()
    {
      if (!has_sent)
        return;
      
      send_data_internal(NULL, 0, true);
      while(posted_rdma_write != num_completed_write) {
        ;
      }
      has_sent = false;
    }

    void sync()
    {
      // printf(">>>>>>>>>>>>>>>>>sync<<<<<<<<<<<<<<<<<\n");
      uint32_t tmp = 0;
      send_data_internal(&tmp, sizeof(uint32_t));
      flush();
      recv_data_internal(&tmp, sizeof(uint32_t));
    }

    void build_connection(struct rdma_cm_id *id)
    {
      // build connection context, call on both client and server.
      conn_context *conn;
      struct ibv_qp_init_attr qp_attr;

      build_context(id->verbs);
      build_qp_attr(&qp_attr);

      TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));
      id->context = conn = (conn_context *)malloc(sizeof(conn_context));

      conn->id = id;
      conn->qp = id->qp;

      conn->send_state = SS_INIT;
      conn->recv_state = RS_INIT;

      conn->connected = 0;

      register_memory(conn);
      post_receive_message(conn, 1, true);
      post_receive_message(conn, RX_BUFFER_DEP - 1);
    }

    void register_memory(struct conn_context *conn)
    {
      conn->send_msg = (message *)malloc(sizeof(struct message) * 2); // extra space for msg_sync
      conn->send_msg[0].type = MSG_MR;
      conn->send_msg[1].type = MSG_DONE;
      conn->recv_msg = (message *)malloc(sizeof(struct message) * 2);

      conn->rdma_local_buffer = (char *) new StreamBuffer();
      conn->rdma_remote_buffer = (char *) new StreamBuffer();
      StreamBuffer *local_sbf = (StreamBuffer *)conn->rdma_local_buffer;
      StreamBuffer *remote_sbf = (StreamBuffer *)conn->rdma_remote_buffer;
      // in this way, we can check wether we can write to the next one
      local_sbf->remote_read_size_total_ptr = &remote_sbf->remote_read_size_total;

      TEST_Z(conn->send_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->send_msg,
                 sizeof(struct message) * 2,
                 0));

      TEST_Z(conn->recv_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->recv_msg,
                 sizeof(struct message) * 2,
                 IBV_ACCESS_LOCAL_WRITE));

      TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->rdma_local_buffer,
                 sizeof(StreamBuffer),
                 0));

      TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->rdma_remote_buffer,
                 sizeof(StreamBuffer),
                 (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE)));
    }

    void build_context(struct ibv_context *verbs)
    {
      if (s_ctx)
      {
        if (s_ctx->ctx != verbs)
          die("cannot handle events in more than one context.");

        return;
      }

      s_ctx = (struct context *)malloc(sizeof(struct context));

      s_ctx->ctx = verbs;

      TEST_Z(
          s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
      TEST_Z(
          s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
      TEST_Z(
          s_ctx->send_cq = ibv_create_cq(s_ctx->ctx, TX_BUFFER_DEP, NULL, s_ctx->comp_channel, 0)); 
      TEST_Z(
          s_ctx->recv_cq = ibv_create_cq(s_ctx->ctx, RX_BUFFER_DEP, NULL, s_ctx->comp_channel, 0));

      TEST_NZ(
          ibv_req_notify_cq(s_ctx->send_cq, 0));
      TEST_NZ(
          ibv_req_notify_cq(s_ctx->recv_cq, 0));
    }

    // struct ibv_cq *cur_cq = nullptr;
    std::queue<struct ibv_wc> cq_wc_queue;

    volatile uint64_t num_completed_write = 0;

    void poll_recv_cq(void *ctx)
    {
      struct ibv_cq *cur_cq;
      struct ibv_wc wc[RX_BUFFER_DEP];
      conn_context *conn = (conn_context *) ctx;

      if (!cq_wc_queue.empty())
      {
        die("I assume wc queue should be empty");
      }
      int poll_num = 0;
      while (1) {
        poll_num = ibv_poll_cq(s_ctx->recv_cq, RX_BUFFER_DEP, wc);
        post_receive_message(conn, poll_num);
        for (uint i = 0; i < poll_num; i++)
          on_completion_flush(&wc[i]);
        
        if (party_done)
          break;
      }
    }

    void poll_snd_cq(void *ctx)
    {
      struct ibv_cq *cur_cq;
      struct ibv_wc wc[TX_BUFFER_DEP];
      conn_context *conn = (conn_context *) ctx;

      if (!cq_wc_queue.empty())
      {
        die("I assume wc queue should be empty");
      }
      int poll_num = 0;
      while (1) {
        poll_num = ibv_poll_cq(s_ctx->send_cq, TX_BUFFER_DEP, wc);
        for (uint i = 0; i < poll_num; i++)
          on_completion_flush(&wc[i]);

        if (party_done)
          break;
      }
    }

    uint32_t posted_recv_num = 0;
    uint32_t recved_msg_num = 0;

    void on_completion_flush(struct ibv_wc *wc)
    {
      if (wc->status != IBV_WC_SUCCESS) {
        printf("wc->status: %d\n", wc->status);
        die("on_completion: wc->status is not IBV_WC_SUCCESS");
      }

      struct conn_context *conn = _conn_context;
      StreamBuffer* remote_sbuf = (StreamBuffer *) conn->rdma_remote_buffer;
      if (wc->opcode == IBV_WC_RECV) {
        // receive MSG_DONE?
        struct message *recv_msg = conn->recv_msg + (recved_msg_num % 2);
        recved_msg_num += 1;

        if (recv_msg->type == MSG_DONE) {
          party_done = true;
        } else {
          printf("recv msg type: %d\n", recv_msg->type);
          die("unexpected message type");
        }
      } else if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
        // get a new message
        recved_msg_num += 1;
        uint data_size = wc->imm_data;
        remote_sbuf->write_size_total += data_size;
      } else if (wc->opcode == IBV_WC_RDMA_WRITE) {
        // RDMA write finished
        num_completed_write += 1;
        completed_send_wr += 1;
      } else if (wc->opcode == IBV_WC_SEND) {
        completed_send_wr += 1;
      } else {
        printf("error: unexpected opcode: %d\n", wc->opcode);
        die("on_completion: opcode is not IBV_WC_SEND or IBV_WC_RECV");
      }
    }

    void send_message(struct conn_context *conn, msg_type type)
    {
      // MSG_MR, MSG_DONE
      struct ibv_send_wr wr;//, *bad_wr = NULL;
      struct ibv_sge sge;

      memset(&wr, 0, sizeof(wr));

      wr.wr_id = (uintptr_t)conn;
      wr.opcode = IBV_WR_SEND;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;

      sge.addr = (uintptr_t)(conn->send_msg + static_cast<int>(type));
      sge.length = sizeof(struct message);
      sge.lkey = conn->send_mr->lkey;

      while (!conn->connected)
        ;

      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
    }

    
    void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
    {
      // build qp attr with inline support, call on both client and server.
      memset(qp_attr, 0, sizeof(*qp_attr));

      qp_attr->send_cq = s_ctx->send_cq;
      qp_attr->recv_cq = s_ctx->recv_cq;
      qp_attr->qp_type = IBV_QPT_RC;

      qp_attr->cap.max_send_wr = TX_BUFFER_DEP;
      qp_attr->cap.max_recv_wr = RX_BUFFER_DEP;
      qp_attr->cap.max_send_sge = 1;
      qp_attr->cap.max_recv_sge = 1;
      qp_attr->cap.max_inline_data = MAX_INLINE_DATA;
    }

    
    int on_connect_request(struct rdma_cm_id *id)
    {
      // call on server, accept client request
      struct rdma_conn_param cm_params;

      printf("received connection request.\n");
      build_connection(id);
      build_params(&cm_params);
      TEST_NZ(rdma_accept(id, &cm_params));

      return 0;
    }


    void build_params(struct rdma_conn_param *params)
    {
      // do not allow wait for rnr (receive not ready)
      memset(params, 0, sizeof(*params));
    }
  };

}

#endif // NETWORK_IO_CHANNEL
