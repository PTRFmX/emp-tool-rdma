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
        uint64_t first_block_size = buffer_len - ppos;
        uint64_t second_block_size = len - first_block_size;
        memcpy((char *)buffer + ppos, data, first_block_size);
        // res.push_back(std::make_pair(ppos, first_block_size));
        pending_writes[0].second += first_block_size;
        memcpy((char *)buffer, data, second_block_size);
        // res.push_back(std::make_pair(0, second_block_size));
        pending_writes[1].second += second_block_size;
        ppos = second_block_size;
      }
      else
      {
        memcpy((char *)buffer + ppos, data, len);
        // res.push_back(std::make_pair(ppos, len));
        pending_writes[0].second += len;
        ppos += len;
        ppos = ppos % buffer_len; // in case ppos is after the last one.
      }
      write_size_total += len;
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
    { // return the size of data can be read, in bytes.
      if (write_size_total < read_size_total)
      {
        printf("error, write is less than read\n");
        die("exited");
      }
      return write_size_total - read_size_total;
    }

    uint64_t available_len()
    {
      if (write_size_total - *remote_read_size_total_ptr > buffer_len)
      {
        printf("error: we have writen too much data to remote\n");
        printf("write size total: %d, remote_read_size_total: %d\n", write_size_total, remote_read_size_total);
        die("exited");
      }
      return write_size_total - *remote_read_size_total_ptr;
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

    void send_data_internal(const void *data, size_t len)
    {
      
      StreamBuffer *buf = (StreamBuffer *)_conn_context->rdma_local_buffer;
      buf->write(data, len);
      // for (auto &wd: wds) {
      //   post_send_data(wd.first, wd.second, _conn_context->id);
      //   // cur_offset += wd.second;
      // }
      has_sent = true;
    }

    uint64_t posted_rdma_write = 0;

    uint64_t posted_send_wr = 0;
    volatile uint64_t completed_send_wr = 0;
    std::mutex send_wr_mutex;
    std::condition_variable send_wr_cv;

    void post_send_data(const uint64_t pos, int len, struct rdma_cm_id *id)
    {

      struct conn_context *conn = (struct conn_context *)id->context;
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
      wr.wr.rdma.remote_addr = (uintptr_t)(remote_sbf->get_addr(pos));
      wr.wr.rdma.rkey = conn->peer_mr.rkey;

      sge.addr = (uintptr_t)(local_sbf->get_addr(pos));
      sge.length = len;
      sge.lkey = conn->rdma_local_mr->lkey;

      while (posted_send_wr - completed_send_wr >= TX_BUFFER_DEP)
        ;

      posted_rdma_write += 1;

      // check wether we can write to the remote side without overwriting unread data
      while(local_sbf->available_len() < len)
        ;

      auto post_res = ibv_post_send(conn->qp, &wr, &bad_wr);
      posted_send_wr += 1;
      if (post_res != 0) {
        printf("ibv_post_send returned: %d\n", post_res);
        printf("errno message: %s\n", strerror(errno));
      }

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

      sge.addr = (uintptr_t)&(local_sbf->write_size_total);
      sge.length = sizeof(local_sbf->write_size_total);
      sge.lkey = conn->rdma_local_mr->lkey;

      while (posted_send_wr - completed_send_wr >= TX_BUFFER_DEP)
        ;

      posted_rdma_write += 1;
      auto post_res = ibv_post_send(conn->qp, &wr, &bad_wr);
      posted_send_wr += 1;
      if (post_res != 0) {
        printf("ibv_post_send returned: %d\n", post_res);
        printf("errno message: %s\n", strerror(errno));
      }
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

      while (posted_send_wr - completed_send_wr >= TX_BUFFER_DEP)
        ;

      posted_rdma_write += 1;
      auto post_res = ibv_post_send(conn->qp, &wr, &bad_wr);
      posted_send_wr += 1;
      if (post_res != 0) {
        printf("ibv_post_send returned: %d\n", post_res);
        printf("errno message: %s\n", strerror(errno));
      }
    }

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
        // printf("rdma write completed\n");
        std::unique_lock<std::mutex> lck(send_wr_mutex);
        completed_send_wr += 1;
        lck.unlock();
        send_wr_cv.notify_one();
        
      }
      else if (wc->opcode == IBV_WC_SEND)
      { // send operation
        // printf("send completed\n");
        std::unique_lock<std::mutex> lck(send_wr_mutex);
        completed_send_wr += 1;
        lck.unlock();
        send_wr_cv.notify_one();
        
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
      // first check the sync num
      sync_num += 1;

      StreamBuffer *local_sbf = (StreamBuffer *)(_conn_context->rdma_local_buffer);
      auto pending_wds = local_sbf->get_pending_writes();

      for (auto &wd : pending_wds) {
        if (wd.second != 0) {
          post_send_data(wd.first, wd.second, _conn_context->id);
        }
      }

      while(posted_rdma_write != num_completed_write) {
        // ibv_poll_cq(s_ctx->send_cq, )
        ;
      }
      end = std::chrono::high_resolution_clock::now();

      post_send_write_len();


      while(posted_rdma_write != num_completed_write) {
        ;
      }
      has_sent = false;
      local_sbf->flush();
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
        completed_send_wr += 1;
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

      while (posted_send_wr - completed_send_wr >= TX_BUFFER_DEP)
        ;
      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
      posted_send_wr += 1;

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
