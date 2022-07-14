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
#define RDMA_BUFFER_SIZE (1UL << 30)
#define UPDATE_READ_FACTOR 10
#define UPDATE_WRITE_FACTOR_SIZE (1UL << 15)
#define UPDATE_WRITE_FACTOR_MSG 128

#define TX_BUFFER_DEP 8192

namespace emp
{

  using write_descriptor = std::pair<uint64_t, size_t>;

  struct ibv_send_wr *bad_wr = NULL;
  class StreamBuffer
  {
  public:
    StreamBuffer()
    {
      buffer_len = RDMA_BUFFER_SIZE;
      // first one: starting from current pos
      // second one: possible need to recurve
      pending_writes.resize(2, std::make_pair<uint64_t, size_t>(0, 0));
      memset((void *)write_size_local, 0, sizeof(write_size_local));
    }

    ~StreamBuffer()
    {

    }

    std::vector<write_descriptor> pending_writes;
    std::vector<write_descriptor> get_pending_writes()
    {
      return pending_writes;
    }

    size_t pending_write_size()
    {
      return pending_writes[0].second + pending_writes[1].second;
    }

    void add_pending_write(size_t len)
    {
      if (pending_writes[0].first + pending_writes[0].second == buffer_len) {
        // adding to the second
        pending_writes[1].second += len;
      } else {
        // adding to the first
        pending_writes[0].second += len;
      }
    }

    void flush()
    {
      pending_writes[0].first = ppos;
      pending_writes[0].second = 0;
      pending_writes[1].first = 0;
      pending_writes[1].second = 0;
    }

    void write(const void *data, size_t len)
    {
      if (pending_write_size() + len > buffer_len)
      {
        printf("buffer space not enough\n");
        die("exited");
      }
      // std::vector<write_descriptor> res;

      if (ppos + len > buffer_len)
      {
        // printf("deal with a wrap\n");
        // printf("%lu, %lu \n", pending_writes[0].first, pending_writes[0].second);
        // printf("%lu \n", ppos);
        // printf("%lu \n", ppos - pending_writes[0].first);
        uint64_t first_block_size = buffer_len - ppos;
        uint64_t second_block_size = len - first_block_size;
        // printf("block size: %lu, %lu\n", first_block_size, second_block_size);
        memcpy((char *)buffer + ppos, data, first_block_size);
        // res.push_back(std::make_pair(ppos, first_block_size));
        // pending_writes[0].second += first_block_size;
        add_pending_write(first_block_size);
        memcpy((char *)buffer, data + first_block_size, second_block_size);
        // res.push_back(std::make_pair(0, second_block_size));
        // pending_writes[1].second += second_block_size;
        add_pending_write(second_block_size);
        ppos = second_block_size;
      }
      else
      {
        memcpy((char *)buffer + ppos, data, len);
        // res.push_back(std::make_pair(ppos, len));
        // pending_writes[0].second += len;
        add_pending_write(len);
        ppos += len;
        ppos = ppos % buffer_len; // in case ppos is after the last one.
      }
      // write_size_total += len;
      uint new_write_index = (write_index + 1) % TX_BUFFER_DEP;
      write_size_local[new_write_index] = *get_write_size_total() + len;
      write_index = new_write_index;
    }

    char *get_addr(uint64_t pos) {
      return (char *)buffer + pos;
    }

    size_t read(void *dst, size_t len)
    {
      if (data_len() < len)
      {
        printf("error, cannot read that much data, available len: %ld\n", data_len());
        die("exited");
      }

      if (gpos + len > buffer_len)
      {
        uint64_t first_block_size = buffer_len - gpos;
        uint64_t second_block_size = len - first_block_size;

        memcpy(dst, (char *)buffer + gpos, first_block_size);
        memcpy(dst + first_block_size, buffer, second_block_size);
        gpos = second_block_size;
      }
      else
      {
        //   memcpy((char *)buffer + ppos, data, len);
        memcpy(dst, (char *)buffer + gpos, len);
        gpos += len;
        gpos = gpos % buffer_len; // in case ppos is after the last one.
      }
      read_size_total += len;
      return len;
    }

    uint64_t data_len()
    { // call from receiver return the size of data can be read.
      if (write_size_total < read_size_total)
      {
        printf("error, write is less than read\n");
        die("exited");
      }
      return write_size_total - read_size_total;
    }

    uint64_t available_len()
    {// call from sender
      if (*get_write_size_total() - *remote_read_size_total_ptr > buffer_len)
      {
        printf("error: we have writen too much data to remote\n");
        printf("write size total: %d, remote_read_size_total: %d\n", *get_write_size_total(), *remote_read_size_total_ptr);
        die("exited");
      }
      return *get_write_size_total() - *remote_read_size_total_ptr;
    }

    volatile uint64_t* get_write_size_total() {
      // printf("write index: %d\n", write_index);
      return &write_size_local[write_index];
    }

    uint64_t gpos = 0;             // read pointer
    uint64_t ppos = 0;             // write pointer
    volatile uint64_t write_size_total = 0; // total data writen, in bytes, need to write to the other side
    volatile uint64_t write_size_local[TX_BUFFER_DEP];
    volatile uint64_t read_size_total = 0;  // total data read, in bytes
    volatile uint64_t remote_read_size_total = 0;
    volatile uint64_t *remote_read_size_total_ptr = nullptr;
    char buffer[RDMA_BUFFER_SIZE];
    size_t buffer_len;
    uint64_t write_index = 0;
  };

  enum msg_type
  {
    MSG_MR,
    MSG_DATA,
    MSG_SYNC,
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
    SS_SYNC_DONE,
    SS_DONE_SENT
  };

  enum r_state
  {
    RS_INIT,
    RS_MR_RECV,
    RS_WAIT_MSG_DATA,
    RS_SYNC_DONE,
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
  };

  class RDMAIO : public IOChannel<RDMAIO>
  {
  public:
    bool is_server;
    string _address;
    int port;

    // party is ready for send & recv
    bool party_ready = false;
    bool party_done = false;
    // party is ready for next send & recv
    bool send_ready = false;
    std::chrono::high_resolution_clock::time_point end;

    uint32_t sync_num = 0;
    uint32_t finished_sync_num = 0;
    std::mutex sync_num_mutex;
    std::condition_variable sync_num_cv;

    bool initialized = false;

    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *client_conn = NULL;
    struct addrinfo *server_addr;
    struct context *s_ctx = NULL;
    struct conn_context *_conn_context = NULL;

    uint cur_offset = 0;

    StreamBuffer *sbuf = nullptr;
    std::mutex sbuf_mutex;
    std::condition_variable sbuf_cv;

    RDMAIO(const char *address, int port, bool quiet = false)
    {
      sbuf = new StreamBuffer();
      is_server = (address == nullptr);
      if (is_server)
      {
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
        while (rdma_get_cm_event(ec, &event) == 0)
        {
          struct rdma_cm_event event_copy;
          memcpy(&event_copy, event, sizeof(*event));
          rdma_ack_cm_event(event);
          if (on_event_server(&event_copy))
            break;
        }
      }
      else
      {
        _address = string(address);
        this->port = port;
        std::string port_ss = std::to_string(port);
        TEST_NZ(getaddrinfo(address, port_ss.c_str(), NULL, &server_addr));
        TEST_Z(ec = rdma_create_event_channel());
        TEST_NZ(rdma_create_id(ec, &client_conn, NULL, RDMA_PS_TCP));
        TEST_NZ(rdma_resolve_addr(client_conn, NULL, server_addr->ai_addr, TIMEOUT_IN_MS));
        freeaddrinfo(server_addr);
        while (rdma_get_cm_event(ec, &event) == 0)
        {
          struct rdma_cm_event event_copy;
          memcpy(&event_copy, event, sizeof(*event));
          rdma_ack_cm_event(event);
          if (on_event_client(&event_copy))
            break;
        }
      }
      init_sndrecv();
      if (!quiet)
        std::cout << "connection establishment finished\n";
    }

    ~RDMAIO()
    { // remember to sync before destructing
      sync();
      if (is_server)
      {
        if (!_conn_context) // no client connected.
          rdma_destroy_id(listener);
        else
        {
          while (rdma_get_cm_event(ec, &event) == 0)
          {
            struct rdma_cm_event event_copy;
            memcpy(&event_copy, event, sizeof(*event));
            rdma_ack_cm_event(event);
            if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED)
            {
              on_disconnect_server((&event_copy)->id);
              break;
            }
            else
            {
              printf("not disconnect event\n");
            }
          }
        }
      }
      else
      {
        // printf("send MSG_DONE\n");
        send_message(_conn_context, MSG_DONE);
        party_done = true;
        rdma_disconnect(_conn_context->id);
        while (rdma_get_cm_event(ec, &event) == 0)
        {
          struct rdma_cm_event event_copy;
          memcpy(&event_copy, event, sizeof(*event));
          rdma_ack_cm_event(event);
          if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED)
          {
            on_disconnect_client((&event_copy)->id);
            break;
          }
          else
          {
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
    size_t write_accumulated = 0;
    size_t write_accumulated_msg = 0;

    void send_data_internal(const void *data, size_t len)
    {
      
      StreamBuffer *buf = (StreamBuffer *)_conn_context->rdma_local_buffer;
      buf->write(data, len);

      StreamBuffer *local_sbf = (StreamBuffer *)(_conn_context->rdma_local_buffer);
      auto pending_wds = local_sbf->get_pending_writes();

      post_send_data(pending_wds, _conn_context->id);
      local_sbf->flush();

      has_sent = true;
      // write_accumulated += len;
      // write_accumulated_msg += 1;
      // if (write_accumulated > UPDATE_WRITE_FACTOR_SIZE || write_accumulated_msg >= UPDATE_WRITE_FACTOR_MSG) {
        // printf("too much data, flush\n");
        // flush();
        // printf("flush finished\n");
      // }
    }

    uint64_t posted_rdma_write = 0;

    // uint64_t posted_send_wr = 0;
    volatile uint64_t completed_send_wr = 0;
    uint64_t post_send_data_time = 0;
    uint64_t wait_remote_read_time = 0;
    uint64_t wait_tx_buffer_time = 0;
    uint64_t set_wr_time = 0;
    uint64_t ibv_post_send_time = 0;

    void post_send_data(std::vector<write_descriptor>& wds, struct rdma_cm_id *id)
    {
      // printf("post send data\n");
      auto start = clock_start();
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
          wr[i].opcode = IBV_WR_RDMA_WRITE;
          wr[i].sg_list = &sge[i];
          wr[i].num_sge = 1;
          wr[i].send_flags = IBV_SEND_SIGNALED;
          wr[i].wr.rdma.remote_addr = (uintptr_t)(remote_sbf->get_addr(wds[i].first));
          wr[i].wr.rdma.rkey = conn->peer_mr.rkey;
          
          sge[i].addr = (uintptr_t)(local_sbf->get_addr(wds[i].first));
          sge[i].length = wds[i].second;
          sge[i].lkey = conn->rdma_local_mr->lkey;
          // posted_send_wr += 1;
          posted_rdma_write += 1;
          last_send_wr = &wr[i];

        }
        if (i > 0 && wds[i].second > 0) {
          wr[i - 1].next = &wr[i];
        }
      }
      // printf("generate send write len\n");

      wr_update_write.wr_id = (uintptr_t)conn;
      wr_update_write.opcode = IBV_WR_RDMA_WRITE;
      wr_update_write.sg_list = &sge[2];
      wr_update_write.num_sge = 1;
      wr_update_write.send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE;
      // printf("check1\n");
      wr_update_write.wr.rdma.remote_addr = (uintptr_t)&(remote_sbf->write_size_total);
      // printf("check2\n");
      wr_update_write.wr.rdma.rkey = conn->peer_mr.rkey;
      // printf("generate write\n");
      sge[2].addr = (uintptr_t)(local_sbf->get_write_size_total());
      sge[2].length = sizeof(local_sbf->write_size_total);
      sge[2].lkey = conn->rdma_local_mr->lkey;
      posted_rdma_write += 1;

      last_send_wr->next = &wr_update_write;

      // auto set_wr = clock_start();

      // check wether we can write to the remote side without overwriting unread data
      size_t len = wds[0].second + wds[1].second;
      while(local_sbf->available_len() < len)
        ;
      // wait_remote_read_time += time_from(set_wr);


      // auto wait_buf_start = clock_start();
      while(posted_rdma_write - num_completed_write >= TX_BUFFER_DEP) {
        ;
      }
      // wait_tx_buffer_time += time_from(wait_buf_start);
      set_wr_time += time_from(start);
      
      auto post_send_start = clock_start();
      auto post_res = ibv_post_send(conn->qp, &wr[0], &bad_wr);
      if (post_res != 0) {
        printf("ibv_post_send returned: %d\n", post_res);
        printf("errno message: %s\n", strerror(errno));
      }
      
      ibv_post_send_time += time_from(post_send_start);
      post_send_data_time += time_from(start);
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


    void post_send_write_len()
    {
      struct conn_context *conn = _conn_context;
      struct ibv_send_wr wr;
      struct ibv_sge sge;

      StreamBuffer *remote_sbf = (StreamBuffer *)conn->peer_mr.addr;
      StreamBuffer *local_sbf = (StreamBuffer *)conn->rdma_local_buffer;

      memset(&wr, 0, sizeof(wr));

      wr.wr_id = (uintptr_t)conn;
      wr.opcode = IBV_WR_RDMA_WRITE;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;
      wr.wr.rdma.remote_addr = (uintptr_t)&(remote_sbf->write_size_total);
      wr.wr.rdma.rkey = conn->peer_mr.rkey;

      sge.addr = (uintptr_t)(local_sbf->get_write_size_total());
      sge.length = sizeof(local_sbf->write_size_total);
      sge.lkey = conn->rdma_local_mr->lkey;
      // printf("make sure all writes finished: %d, %d\n", posted_rdma_write, num_completed_write);
      while(posted_rdma_write != num_completed_write) {
        ;
      }
      // printf("wait finished\n");

      // while (posted_send_wr - completed_send_wr >= TX_BUFFER_DEP)
        // ;

      auto post_res = ibv_post_send(conn->qp, &wr, &bad_wr);
      posted_rdma_write += 1;
      // posted_send_wr += 1;
      if (post_res != 0) {
        printf("ibv_post_send returned: %d\n", post_res);
        printf("errno message: %s\n", strerror(errno));
      }
      // printf("return\n");
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
      wr.send_flags = IBV_SEND_SIGNALED;
      wr.wr.rdma.remote_addr = (uintptr_t)&(remote_sbf->remote_read_size_total);
      wr.wr.rdma.rkey = conn->peer_mr.rkey;

      sge.addr = (uintptr_t)&(local_sbf->read_size_total);
      sge.length = sizeof(local_sbf->read_size_total);
      sge.lkey = conn->rdma_remote_mr->lkey;

      // while (posted_send_wr - completed_send_wr >= TX_BUFFER_DEP)
        // ;
      while(posted_rdma_write - num_completed_write >= TX_BUFFER_DEP) {
        ;
      }

      posted_rdma_write += 1;
      auto post_res = ibv_post_send(conn->qp, &wr, &bad_wr);
      // posted_send_wr += 1;
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
      
      remote_sbf->read(data, len);
      read_accumulated += len;
      post_send_read_len();

    }

    int on_addr_resolved(struct rdma_cm_id *id)
    {

      printf("address resolved.\n");

      build_connection(id);
      TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

      return 0;
    }

    void register_memory(struct conn_context *conn)
    {
      conn->send_msg = (message *)malloc(sizeof(struct message) * 4); // extra space for msg_sync
      conn->send_msg[0].type = MSG_MR;
      conn->send_msg[1].type = MSG_DATA;
      conn->send_msg[2].type = MSG_SYNC;
      conn->send_msg[3].type = MSG_DONE;
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
                 sizeof(struct message) * 4,
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

    int on_route_resolved(struct rdma_cm_id *id)
    {
      struct rdma_conn_param cm_params;

      printf("route resolved.\n");
      build_params(&cm_params);
      TEST_NZ(rdma_connect(id, &cm_params));
      // printf("connect done.\n");
      return 0;
    }

    void send_mr(void *context)
    {
      struct conn_context *conn = (struct conn_context *)context;

      for (uint i = 0; i < 4; i++)
        memcpy(&conn->send_msg[i].data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));
      send_message(conn, MSG_MR);
    }

    uint32_t num_sync()
    {
      return finished_sync_num;
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

    void init_sndrecv()
    {
      poll_cq_connect(nullptr);
      post_receive_message(_conn_context);
      s_ctx->cq_poller_thread = new std::thread([this]()
                                                { poll_cq_flush(nullptr); });
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

    void post_receive_message(struct conn_context *conn)
    {
      struct ibv_recv_wr wr, *recv_bad_wr = NULL;
      struct ibv_sge sge;

      wr.wr_id = (uintptr_t)conn;
      wr.next = NULL;
      wr.sg_list = &sge;
      wr.num_sge = 1;

      sge.addr = (uintptr_t)(conn->recv_msg + (posted_recv_num % 2));
      sge.length = sizeof(struct message);
      sge.lkey = conn->recv_mr->lkey;
      // printf("post receive, at message index: %d\n", posted_recv_num % 2);
      TEST_NZ(ibv_post_recv(conn->qp, &wr, &recv_bad_wr));
      posted_recv_num += 1;
      // printf("post receive finished\n");
    }

    void on_completion(struct ibv_wc *wc, bool is_server)
    { // it only deals with messages
      struct conn_context *conn = (struct conn_context *)(uintptr_t)wc->wr_id;

      if (wc->status != IBV_WC_SUCCESS)
      {
        printf("error status: %d\n", wc->status);
        die("on_completion: status is not IBV_WC_SUCCESS.");
      }

      if (wc->opcode & IBV_WC_RECV)
      {
        recved_msg_num += 1;
        // printf("recved message, type: %d\n", conn->recv_msg->type);

        if (conn->recv_msg->type == MSG_MR)
        {
          printf("received MR.\n");
          if (conn->recv_state == RS_INIT)
            conn->recv_state = RS_MR_RECV;
          memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));

          // if (is_server)
          post_receive_message(conn);
          if (conn->send_state == SS_INIT)
          {
            printf("send mr\n");
            send_mr(conn);
          }
        }
        else if (conn->recv_msg->type == MSG_DONE)
        {
          printf("received MSG_DONE\n");
          conn->recv_state = RS_DONE_RECV;
          conn->send_state = SS_DONE_SENT;
        }
        else if (conn->recv_msg->type == MSG_DATA)
        {
          die("It should not recv MSG_DATA");
        }
        else if (conn->recv_msg->type == MSG_SYNC)
        { // only server will recv this
          
          post_receive_message(conn);
          std::unique_lock<std::mutex> lk(sync_num_mutex);
          finished_sync_num += 1;
          sync_num_cv.notify_one();
          lk.unlock();
        }
        else
        {
          printf("unkown message type: %d\n", conn->recv_msg->type);
          die("on_completion: unknown message type.");
        }
      }
      else if (wc->opcode == IBV_WC_RDMA_WRITE)
      {

        
      }
      else if (wc->opcode == IBV_WC_SEND)
      { // send operation
        
        if (conn->send_state == SS_INIT)
        {
          conn->send_state = SS_MR_SENT;
          send_ready = true;
        }
        else if (conn->send_state == SS_MR_SENT)
        {

        }
      }
      else
      { // other opcodes
        printf("unknown opcode: %d\n", wc->opcode);
        die("on_completion: unknown opcode.");
      }

      if (conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV && !initialized)
      {
        // no need to wait
        party_ready = true;
        initialized = true;
      }
      if (conn->send_state == SS_DONE_SENT && conn->recv_state == RS_DONE_RECV && is_server)
      {
        party_done = true;
        printf("disconnecting\n");
        rdma_disconnect(conn->id);
      }
    }

    void flush()
    {
      if (!has_sent)
        return;
      // first check the sync num
      sync_num += 1;

      // StreamBuffer *local_sbf = (StreamBuffer *)(_conn_context->rdma_local_buffer);
      // auto pending_wds = local_sbf->get_pending_writes();

      // // for (auto &wd : pending_wds) {
      // //   if (wd.second != 0) {
      // //     post_send_data(wd.first, wd.second, _conn_context->id);
      // //   }
      // // }
      // post_send_data(pending_wds, _conn_context->id);

      // while(posted_rdma_write != num_completed_write) {
      //   // ibv_poll_cq(s_ctx->send_cq, )
      //   ;
      // }
      // end = std::chrono::high_resolution_clock::now();

      // post_send_write_len();


      has_sent = false;
      // local_sbf->flush();
      write_accumulated = 0;
      write_accumulated_msg = 0;
      while(posted_rdma_write != num_completed_write) {
        ;
      }
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
      post_receive_message(conn);
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
          s_ctx->send_cq = ibv_create_cq(s_ctx->ctx, TX_BUFFER_DEP, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
      // TEST_Z(
          // s_ctx->recv_cq = ibv_create_cq(s_ctx->ctx, TX_BUFFER_DEP, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
      s_ctx->recv_cq = s_ctx->send_cq;
      TEST_NZ(
          ibv_req_notify_cq(s_ctx->send_cq, 0));
      TEST_NZ(
          ibv_req_notify_cq(s_ctx->recv_cq, 0));
      // TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, RDMAIO::poll_cq, NULL));
      // s_ctx->cq_poller_thread = new std::thread([this]()
      // { poll_cq_connect(NULL); });
    }

    void poll_cq_connect(void *ctx)
    {
      struct ibv_cq *cq;
      struct ibv_wc wc;

      while (1)
      {
        if (!cq_wc_queue.empty())
        {
          die("I assume cq_wc_queue is empty");
        }
        // printf("get cq event\n");
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        // printf("ack cq event\n");
        ibv_ack_cq_events(cq, 1);
        // printf("notify cq\n");
        TEST_NZ(ibv_req_notify_cq(cq, 0));
        // printf("prepare to poll cq\n");

        int num_comp = ibv_poll_cq(cq, 1, &wc);
        while (num_comp && !party_ready)
        {
          // printf("num_comp: %d\n", num_comp);
          on_completion(&wc, is_server);
          // printf("poll cq again, party ready: %d\n", party_ready);
          num_comp = ibv_poll_cq(cq, 1, &wc);
          // printf("num_comp: %d\n", num_comp);
        }

        if (num_comp)
        {
          cq_wc_queue.push(wc);
          while (ibv_poll_cq(cq, 1, &wc))
          {
            cq_wc_queue.push(wc);
          }
        }

        if (party_ready)
        {
          printf("party ready\n");
          break;
        }
      }
    }

    // struct ibv_cq *cur_cq = nullptr;
    std::queue<struct ibv_wc> cq_wc_queue;

    volatile uint64_t num_completed_write = 0;

    void poll_cq_flush(void *ctx)
    {
      struct ibv_cq *cur_cq;
      struct ibv_wc wc;

      if (!cq_wc_queue.empty())
      {
        die("I assume wc queue should be empty");
      }

      // while (1)
      // {
      //   TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cur_cq, &ctx));
      //   ibv_ack_cq_events(cur_cq, 1);
      //   TEST_NZ(ibv_req_notify_cq(cur_cq, 0));
      //   while (ibv_poll_cq(cur_cq, 1, &wc))
      //   {
      //     on_completion_flush(&wc);
      //   }

      //   if (party_done)
      //     break;
      // }
      while (1)
      {
        // TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cur_cq, &ctx));
        // ibv_ack_cq_events(cur_cq, 1);
        // TEST_NZ(ibv_req_notify_cq(cur_cq, 0));
        while (ibv_poll_cq(s_ctx->recv_cq, 1, &wc))
        {
          on_completion_flush(&wc);
        }

        if (party_done)
          break;
      }
    }
    uint32_t posted_recv_num = 0;
    uint32_t recved_msg_num = 0;

    void on_completion_flush(struct ibv_wc *wc)
    {
      if (wc->status != IBV_WC_SUCCESS)
      {
        printf("wc->status: %d\n", wc->status);
        die("on_completion: wc->status is not IBV_WC_SUCCESS");
      }

      if (wc->opcode == IBV_WC_RECV)
      {
        // receive MSG_DATA or MSG_SYNC
        struct conn_context *conn = (struct conn_context *)wc->wr_id;
        struct message *recv_msg = conn->recv_msg + (recved_msg_num % 2);
        // printf("select recv msg from index: %d\n", recved_msg_num % 2);
        recved_msg_num += 1;

        if (recv_msg->type == MSG_DATA)
        {
          printf("receive deprecated msg type\n");
          die("failed\n");
          // proceed data
          // sbuf_mutex.lock();
          
        }
        else if (recv_msg->type == MSG_SYNC)
        {
          printf("receive deprecated msg type\n");
          die("failed\n");
          // post_receive_message(conn);
        }
        else if (recv_msg->type == MSG_DONE)
        {
          party_done = true;
        }
        else
        {
          printf("recv msg type: %d\n", recv_msg->type);
          die("unexpected message type");
        }
      }
      else if (wc->opcode == IBV_WC_RDMA_WRITE)
      {
        // RDMA write finished
        num_completed_write += 1;
        completed_send_wr += 1;
      }
      else if (wc->opcode == IBV_WC_SEND)
      {
        // completed_send_wr += 1;
      }
      else
      {
        printf("error: unexpected opcode: %d\n", wc->opcode);
        die("on_completion: opcode is not IBV_WC_SEND or IBV_WC_RECV");
      }
    }
    uint32_t pending_rdma_write = 0;

    void send_message(struct conn_context *conn, msg_type type)
    {
      // MSG_DATA, MSG_MR, MSG_DONE
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
      if (type == MSG_DATA)
      {
        printf("MSG DATA is deprecated\n");
        die("failed\n");
      }

      while (!conn->connected)
        ;

      // while (posted_send_wr - completed_send_wr >= TX_BUFFER_DEP)
        // ;
      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
      // posted_send_wr += 1;

    }

    void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
    {
      memset(qp_attr, 0, sizeof(*qp_attr));

      qp_attr->send_cq = s_ctx->send_cq;
      qp_attr->recv_cq = s_ctx->recv_cq;
      qp_attr->qp_type = IBV_QPT_RC;

      qp_attr->cap.max_send_wr = 8000;
      qp_attr->cap.max_recv_wr = 100;
      qp_attr->cap.max_send_sge = 1;
      qp_attr->cap.max_recv_sge = 1;
    }

    int on_connect_request(struct rdma_cm_id *id)
    {
      struct rdma_conn_param cm_params;

      printf("received connection request.\n");
      build_connection(id);
      build_params(&cm_params);
      TEST_NZ(rdma_accept(id, &cm_params));

      return 0;
    }


    void build_params(struct rdma_conn_param *params)
    {
      memset(params, 0, sizeof(*params));
    }
  };

}

#endif // NETWORK_IO_CHANNEL
