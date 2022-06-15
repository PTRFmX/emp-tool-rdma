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
    if ((x)) {                                            \
      printf("errno: %s\n", strerror(errno));           \
      die("error: " #x " failed (returned non-zero)."); \
    }                                                    \
  } while (0)
#define TEST_Z(x)                                        \
  do                                                     \
  {                                                      \
    if (!(x)) {                                            \
      printf("errno: %s\n", strerror(errno));           \
      die("error: " #x " failed (returned zero/null)."); \
    }                                                    \
  } while (0)

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

const int TIMEOUT_IN_MS = 500; /* ms */
const size_t RDMA_BUFFER_SIZE = 1 << 11;

#define MAX_MESSAGE_NUM 10
uint poll_index = 0;

namespace emp
{

  // class CEvent
  // {
  // public:
  //   CEvent(bool bManualReset = false, bool bInitialSet = false) : m_bManual(bManualReset), m_bSet(bInitialSet)
  //   {
  //   }
  //   ~CEvent() = default;

  //   bool Set()
  //   {
  //     std::unique_lock<std::mutex> lock(mutex_);
  //     if (m_bSet)
  //       return true;

  //     m_bSet = true;
  //     lock.unlock();
  //     cv_.notify_one();
  //     return true;
  //   }

  //   bool Wait()
  //   {
  //     std::unique_lock<std::mutex> lock(mutex_);
  //     cv_.wait(lock, [this]
  //              { return m_bSet; });

  //     if (!m_bManual)
  //       m_bSet = false;
  //     return true;
  //   }
  //   bool IsSet() const
  //   {
  //     std::lock_guard<std::mutex> lock(mutex_);
  //     return m_bSet;
  //   }
  //   bool Reset()
  //   {
  //     std::lock_guard<std::mutex> lock(mutex_);
  //     m_bSet = false;
  //     return true;
  //   }

  // private:
  //   std::condition_variable cv_;
  //   mutable std::mutex mutex_;
  //   bool m_bManual;
  //   bool m_bSet;
  // };


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
    uint32_t size[MAX_MESSAGE_NUM];
  };

  enum s_state
  {
    SS_INIT,
    SS_MR_SENT,
    SS_RDMA_SENT,
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
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    std::thread *cq_poller_thread;
  };

  struct recv_task
  {
    recv_task(void *data, size_t len) : data(data), len(len) {}
    recv_task()
    {
      data = nullptr;
      len = 0;
    }
    void *data;
    size_t len;
  };

  class RDMAIO : public IOChannel<RDMAIO>
  {
  public:
    bool is_server;
    string _address;
    int port;

    // party is ready for send & recv
    bool party_ready = false;
    std::mutex party_ready_mutex;
    std::condition_variable party_ready_cv;
    bool party_done = false;
    // party is ready for next send & recv
    bool send_ready = false;
    std::mutex send_ready_mutex;
    std::condition_variable send_ready_cv;

    uint32_t posted_msg_num = 0;
    uint32_t completed_msg_num = 0;
    std::mutex msg_num_mutex;
    std::condition_variable msg_num_cv;

    bool sync_done = false;
    std::mutex sync_done_mutex;
    std::condition_variable sync_done_cv;

    uint32_t sync_num = 0;
    uint32_t finished_sync_num = 0;
    std::mutex sync_num_mutex;
    std::condition_variable sync_num_cv;

    std::mutex send_msg_mutex;

    bool initialized = false;
    bool posted_sync = false;

    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *client_conn = NULL;
    struct addrinfo *server_addr;
    struct context *s_ctx = NULL;
    struct conn_context *_conn_context = NULL;
    std::queue<recv_task> recv_queue;
    std::mutex recv_queue_mutex;
    std::condition_variable recv_queue_cv;

    uint cur_offset = 0;
    uint cur_message_index = 0;

    RDMAIO(const char *address, int port, bool quiet = false)
    {
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
      if (!quiet)
        std::cout << "connection establishment finished\n";
      init_sndrecv();
    }

    ~RDMAIO()
    { // remember to sync before destructing
      // sync();
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
            } else {
              printf("not disconnect event\n");
            }
          }
        }
      }
      else
      {
        // printf("send msg done\n");
        // send_msg_mutex.lock();
        // _conn_context->send_msg[0].type = MSG_DONE;
        printf("send MSG_DONE\n");
        send_message(_conn_context, MSG_DONE);
        // send_msg_mutex.unlock();
        // s_ctx->cq_poller_thread->join();
        // printf("client disconnect\n");
        // rdma_disconnect(_conn_context->id);
        // polling for disconnect
        while (rdma_get_cm_event(ec, &event) == 0)
        {
          struct rdma_cm_event event_copy;
          memcpy(&event_copy, event, sizeof(*event));
          rdma_ack_cm_event(event);
          if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED)
          {
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
      // printf("send mr\n");
      send_mr(id->context);
      // printf("set connected\n");
      return 1; // break loop
    }

    int on_connection_client(struct rdma_cm_id *id)
    {
      _conn_context = (struct conn_context *)id->context;
      _conn_context->connected = 1;
      // printf("send mr\n");
      send_mr(id->context);
      // printf("posted send mr\n");
      return 1;
    }

    void send_data_internal(const void *data, size_t len)
    {
      // printf("send data\n");
      if (cur_offset + len > RDMA_BUFFER_SIZE)
      {
        sync();
      }
      std::unique_lock<std::mutex> lck(send_ready_mutex);
      send_ready_cv.wait(lck, [this] { return send_ready; });
      post_send_data(data, len, _conn_context->id, cur_offset);
      // printf("cur_offset: %d\n", cur_offset);
      cur_offset += len;
      lck.unlock();
    }

    void post_send_data(const void *data, int len, struct rdma_cm_id *id, uint32_t offset)
    {

      struct conn_context *conn = (struct conn_context *)id->context;
      struct ibv_send_wr wr, *bad_wr = NULL;
      struct ibv_sge sge;

      memcpy(((char *)conn->rdma_local_buffer + offset), data, len);
      memset(&wr, 0, sizeof(wr));

      wr.wr_id = (uintptr_t)conn;
      wr.opcode = IBV_WR_RDMA_WRITE;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;
      wr.wr.rdma.remote_addr = (uintptr_t)((char *)conn->peer_mr.addr + offset);
      wr.wr.rdma.rkey = conn->peer_mr.rkey;

      sge.addr = (uintptr_t)((char *)conn->rdma_local_buffer + offset);
      sge.length = len;
      sge.lkey = conn->rdma_local_mr->lkey;
      // printf("post send rdma write\n");
      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
      conn->send_msg[1].size[cur_message_index] = len; // MSG_DATA = 1
      cur_message_index += 1;
      conn->send_msg[1].size[cur_message_index] = 0; // set the next size as 0 to indicate the end of the message.
      
    }

    void recv_data_internal(void *data, size_t len)
    {
      // printf("recv data\n");
      // blocking
      recv_task task;
      // while (1)
      // {
      //   recv_queue_mutex.lock();
      //   if (recv_queue.empty())
      //   {
      //     recv_queue_mutex.unlock();
      //     continue;
      //   }
      //   task = recv_queue.front();
      //   recv_queue.pop();
      //   recv_queue_mutex.unlock();
      //   break;
      // }
      std::unique_lock<std::mutex> lck(recv_queue_mutex);
      recv_queue_cv.wait(lck, [this] { return !recv_queue.empty(); });
      // printf("recv queue size: %d\n", recv_queue.size());
      task = recv_queue.front();
      recv_queue.pop();
      // printf("recv queue size: %d\n", recv_queue.size());
      memcpy(data, task.data, task.len);
      // printf("task len: %d\n", task.len);
      free(task.data);
      lck.unlock();
      // printf("finished receives, task size: %ld\n", len);
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
      conn->recv_msg = (message *)malloc(sizeof(struct message));

      conn->rdma_local_buffer = (char *)malloc(RDMA_BUFFER_SIZE);
      conn->rdma_remote_buffer = (char *)malloc(RDMA_BUFFER_SIZE);

      TEST_Z(conn->send_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->send_msg,
                 sizeof(struct message) * 4,
                 0));

      TEST_Z(conn->recv_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->recv_msg,
                 sizeof(struct message),
                 IBV_ACCESS_LOCAL_WRITE));

      TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->rdma_local_buffer,
                 RDMA_BUFFER_SIZE,
                 0));

      TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
                 s_ctx->pd,
                 conn->rdma_remote_buffer,
                 RDMA_BUFFER_SIZE,
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
      // send_msg_mutex.lock();
      // conn->send_msg[0].type = MSG_MR;
      for (uint i = 0; i < 4; i++)
        memcpy(&conn->send_msg[i].data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));
      // printf("prepare to send mr message\n");
      send_message(conn, MSG_MR);
      // send_msg_mutex.unlock();
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
      s_ctx->cq_poller_thread->join();
      delete s_ctx->cq_poller_thread;
      s_ctx->cq_poller_thread = new std::thread([this]
                                                { poll_cq_sndrecv(nullptr); });
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
      struct ibv_recv_wr wr, *bad_wr = NULL;
      struct ibv_sge sge;

      wr.wr_id = (uintptr_t)conn;
      wr.next = NULL;
      wr.sg_list = &sge;
      wr.num_sge = 1;

      sge.addr = (uintptr_t)(conn->recv_msg);
      sge.length = sizeof(struct message);
      sge.lkey = conn->recv_mr->lkey;
      // printf("post receive\n");
      TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
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
        // printf("recved message, type: %d\n", conn->recv_msg->type);

        if (conn->recv_msg->type == MSG_MR)
        {
          if (conn->recv_state == RS_INIT)
            conn->recv_state = RS_MR_RECV;
          memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
          post_receive_message(conn);
          // if (conn->send_state == SS_INIT)
          // {
          //   printf("send mr\n");
          //   send_mr(conn);
          // }
        }
        else if (conn->recv_msg->type == MSG_DONE)
        {
          printf("received MSG_DONE\n");
          conn->recv_state = RS_DONE_RECV;
          conn->send_state = SS_DONE_SENT;
        }
        else if (conn->recv_msg->type == MSG_DATA)
        {
          // printf("recv MSG_DATA\n");
          // uint size = conn->recv_msg->size;
          // void *data = malloc(size);
          // uint offset = conn->recv_msg->offset;
          // memcpy(data, (char *)conn->rdma_remote_buffer + offset, size);
          // auto task = recv_task(data, size);
          uint cur_recv_offset = 0;
          std::unique_lock<std::mutex> lk(recv_queue_mutex);
          for (uint i = 0; i < MAX_MESSAGE_NUM; i++) {
            uint size = conn->recv_msg->size[i];
            if (size == 0) {
              // printf("received %d messages\n", i);
              break;
            }
            void *data = malloc(size);
            memcpy(data, (char *)conn->rdma_remote_buffer + cur_recv_offset, size);
            recv_task task(data, size);
            // printf("push task \n");
            recv_queue.push(task);
            cur_recv_offset += size;
          }
          lk.unlock();
          recv_queue_cv.notify_one();
          post_receive_message(conn);
          // send_msg_mutex.lock();
          // conn->send_msg[1].type = MSG_SYNC;
          // printf("send MSG_SYNC\n");
          if (is_server)
            send_message(conn, MSG_DATA);
          else {
            send_message(conn, MSG_SYNC);
            posted_sync = true;
          }
          // send_msg_mutex.unlock();
        }
        else if (conn->recv_msg->type == MSG_SYNC) {
          // printf("recv MSG_SYNC\n");
          // std::unique_lock<std::mutex> lk(sync_done_mutex);
          // sync_done = true;
          // sync_done_cv.notify_one();
          // lk.unlock();
          std::unique_lock<std::mutex> lk(sync_num_mutex);
          finished_sync_num += 1;
          sync_num_cv.notify_one();
          lk.unlock();
          post_receive_message(conn);
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
      }
      else if (wc->opcode == IBV_WC_SEND)
      { // send operation
        // printf("send completed\n");
        if (conn->send_state == SS_INIT)
        {
          conn->send_state = SS_MR_SENT;
        }
        else if (conn->send_state == SS_MR_SENT)
        {
          std::unique_lock<std::mutex> send_ready_lock(send_ready_mutex);
          send_ready = true;
          send_ready_lock.unlock();
          send_ready_cv.notify_one();
          // printf("update completed msg num\n");
        }
        if (!is_server && posted_sync)
        {
          // std::unique_lock<std::mutex> lk(sync_done_mutex);
          // sync_done = true;
          // sync_done_cv.notify_one();
          // lk.unlock();
          std::unique_lock<std::mutex> lk(sync_num_mutex);
          finished_sync_num += 1;
          sync_num_cv.notify_one();
          lk.unlock();
          posted_sync = false;
        }
        // std::unique_lock<std::mutex> msg_num_lock(msg_num_mutex);
        // completed_msg_num++;
        // msg_num_cv.notify_one();
        // msg_num_lock.unlock();
      }
      else
      {// other opcodes
        printf("unknown opcode: %d\n", wc->opcode);
        die("on_completion: unknown opcode.");
      }

      if (conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV && !initialized) {
        std::unique_lock<std::mutex> party_ready_lock(party_ready_mutex);
        // no need to wait
        party_ready = true;
        party_ready_lock.unlock();
        party_ready_cv.notify_one();
        initialized = true;

        std::unique_lock<std::mutex> send_ready_lock(send_ready_mutex);
        send_ready = true;
        send_ready_lock.unlock();
        send_ready_cv.notify_one();
      }
      if (conn->send_state == SS_DONE_SENT && conn->recv_state == RS_DONE_RECV && is_server)
      {
        party_done = true;
        printf("disconnecting\n");
        rdma_disconnect(conn->id);
      }
    }

    void sync()
    {
      printf("sync\n");
      sync_num += 1;
      flush();
      // printf("waiting for sync done\n");
      std::unique_lock<std::mutex> lk(sync_num_mutex);
      sync_num_cv.wait(lk, [this] { return sync_num == finished_sync_num; });
      lk.unlock();
      printf("sync finished\n");
    }

    void flush()
    { // ensure that all data is sent
      // printf("flush\n");
      // send_ready_mutex.lock();
      // while (true)
      // {
      //   send_ready_mutex.lock();
      //   if (num_send_task == 0)
      //   {
      //     send_ready_mutex.unlock();
      //     break;
      //   }
      //   else
      //     send_ready_mutex.unlock();
      // }
      // cur_offset = 0;
      // printf("flush done\n");
      // send_msg_mutex.lock();
      // _conn_context->send_msg[0].type=MSG_DATA;
      // printf("send MSG_DATA\n");
      if (!is_server) {
        send_message(_conn_context, MSG_DATA);
        cur_message_index = 0;
        cur_offset = 0;
        // _conn_context->send_msg[1].size[0] = 0;
      }
      // send_msg_mutex.unlock();
      // send again to make sure that all data is received and stored in buffer
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
          s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
      TEST_NZ(
          ibv_req_notify_cq(s_ctx->cq, 0));
      // TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, RDMAIO::poll_cq, NULL));
      s_ctx->cq_poller_thread = new std::thread([this]()
                                                { poll_cq_connect(NULL); });
    }

    void poll_cq_connect(void *ctx)
    {
      struct ibv_cq *cq;
      struct ibv_wc wc;

      while (1)
      {
        // printf("get cq event\n");
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));
        // printf("prepare to poll cq\n");
        int num_comp = ibv_poll_cq(cq, 1, &wc);

        while (num_comp)
        {
          // printf("num_comp: %d\n", num_comp);
          on_completion(&wc, is_server);
          // printf("poll cq\n");
          num_comp = ibv_poll_cq(cq, 1, &wc);
          // printf("num_comp: %d\n", num_comp);
        }

        if (party_ready)
        {
          // printf("party ready\n");
          break;
        }
      }
    }

    void poll_cq_sndrecv(void *ctx)
    {
      struct ibv_cq *cq;
      struct ibv_wc wc;
      while (1)
      {
        // printf("get cq event\n");
        // auto start = std::chrono::high_resolution_clock::now();
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));
        // auto end = std::chrono::high_resolution_clock::now();
        // printf("=======================index: %d poll cq time: %ld us===================\n", poll_index, std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
        poll_index++;
        // printf("prepare to poll cq\n");
        int num_comp = ibv_poll_cq(cq, 1, &wc);

        while (num_comp)
        {
          on_completion(&wc, is_server);
          // printf("poll cq\n");
          num_comp = ibv_poll_cq(cq, 1, &wc);
          // printf("num_comp: %d\n", num_comp);
        }

        if (party_done)
          break;
      }
    }

    void send_message(struct conn_context *conn, msg_type type)
    {
      // MSG_DATA, MSG_MR, MSG_DONE
      struct ibv_send_wr wr, *bad_wr = NULL;
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

      
      // while (true)
      // {
      //   send_ready_mutex.lock();
      //   if (send_ready || conn->send_state == SS_INIT)
      //     break;
      //   else
      //   {
      //     send_ready_mutex.unlock();
      //     continue;
      //   }
      // }
      // revise this part to event driven
      // printf("check 1\n");
      // printf("wait for send ready\n");
      // std::unique_lock<std::mutex> lck(send_ready_mutex);
      // send_ready_cv.wait(lck, [this, conn](){ return send_ready ||!initialized; });
      // if (initialized)
        // send_ready = false;
      // printf("post send\n");
      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
      // lck.unlock();
      // printf("check 2\n");

      // wait for send_ready
      // std::unique_lock<std::mutex> lck2(send_ready_mutex);
      // printf("check 2.1\n");
      // send_ready_cv.wait(lck2, [this, conn](){ return send_ready ||!initialized; });
      // printf("check 2.2\n");
      // lck2.unlock();
      // cur_message_index = 0;
      // printf("check 3\n");
      // printf("wait for this message finished\n");
      // posted_msg_num++;
      // std::unique_lock<std::mutex> lck2(msg_num_mutex);
      // // printf("posted msg num: %d, completed msg num: %d\n", posted_msg_num, completed_msg_num);
      // msg_num_cv.wait(lck2, [this, conn](){ return posted_msg_num == completed_msg_num; });
      // lck2.unlock();

      // printf("posted send msg data\n");
      // printf("unset send_ready\n");
      // send_ready = false;
      // num_send_task += 1;
      // send_ready_mutex.unlock();
    }

    void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
    {
      memset(qp_attr, 0, sizeof(*qp_attr));

      qp_attr->send_cq = s_ctx->cq;
      qp_attr->recv_cq = s_ctx->cq;
      qp_attr->qp_type = IBV_QPT_RC;

      qp_attr->cap.max_send_wr = 10;
      qp_attr->cap.max_recv_wr = 10;
      qp_attr->cap.max_send_sge = 1;
      qp_attr->cap.max_recv_sge = 1;
    }

    int on_connect_request(struct rdma_cm_id *id)
    {
      struct rdma_conn_param cm_params;

      printf("received connection request.\n");
      build_connection(id);
      build_params(&cm_params);
      // sprintf((char *)get_local_message_region(id->context), "message from passive/server side with pid %d", getpid());
      TEST_NZ(rdma_accept(id, &cm_params));

      return 0;
    }

    void *get_local_message_region(void *context)
    {
      return ((struct conn_context *)context)->rdma_local_buffer;
    }

    void *get_peer_message_region(struct conn_context *conn)
    {
      return conn->rdma_remote_buffer;
    }

    void build_params(struct rdma_conn_param *params)
    {
      memset(params, 0, sizeof(*params));

      params->initiator_depth = params->responder_resources = 1;
      params->rnr_retry_count = 7; /* infinite retry */
    }
  };

}

#endif // NETWORK_IO_CHANNEL
