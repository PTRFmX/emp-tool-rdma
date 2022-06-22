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
const size_t RDMA_BUFFER_SIZE = 1 << 30;

#define MAX_MESSAGE_NUM 100
uint poll_index = 0;

namespace emp
{

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
    SS_WAIT_MSG_DATA,
    SS_SYNC_DONE,
    SS_DONE_SENT
  };

  enum sync_state {
    SYNC_WAIT_MSG_DATA,
    SYNC_WAIT_MSG_SYNC,
    SYNC_WAIT_SEND_DONE,
    SYNC_DONE
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

    bool flush_start = false;
    std::mutex flush_start_mutex;
    std::condition_variable flush_start_cv;

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
      init_sndrecv();
      if (!quiet)
        std::cout << "connection establishment finished\n";
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
        // printf("send MSG_DONE\n");
        // send_message(_conn_context, MSG_DONE);
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
      // send_mr(id->context);
      // printf("set connected\n");
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

    void send_data_internal(const void *data, size_t len)
    {
      // printf("send data\n");
      if (cur_offset + len > RDMA_BUFFER_SIZE)
      {
        die("send_data: buffer overflow.");
      }
      // printf("send data %d\n", len);
      // std::unique_lock<std::mutex> lck(send_ready_mutex);
      // send_ready_cv.wait(lck, [this] { return send_ready; });
      post_send_data(data, len, _conn_context->id, cur_offset);
      // printf("cur_offset: %d\n", cur_offset);
      cur_offset += len;
      if (len == 4) {
        printf("pushed a pointer length of %d\n", *(int *)data);
        printf("at offset: %d\n", cur_offset - len);
      } else {
        printf("pushed data length: %d\n", len);
        printf("at offset: %d\n", cur_offset - len);
      }
      // lck.unlock();
      // printf("posted send data\n");
    }

    uint pending_rdma_write = 0;

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
      printf("post send first byte: %d\n", *(int *)data);
      // printf("set send_msg: %d, %d, at offset: %d\n", cur_message_index, len, offset);
      conn->send_msg[1].size[cur_message_index] = len; // MSG_DATA = 1
      cur_message_index += 1;
      conn->send_msg[1].size[cur_message_index] = 0; // set the next size as 0 to indicate the end of the message.
      pending_rdma_write += 1;
    }

    std::queue<std::thread> recv_threads_queue;
    std::queue<recv_task> pending_recv_task_queue;
    std::mutex pending_recv_task_mutex;
    void recv_data_internal(void *data, size_t len)
    {
      // auto recv_func = [this, data, len] () {
      //   recv_task task;
      //   std::unique_lock<std::mutex> lck(recv_queue_mutex);
      //   recv_queue_cv.wait(lck, [this] { return !recv_queue.empty(); });
      //   // printf("recv queue size: %d\n", recv_queue.size());
      //   task = recv_queue.front();
      //   recv_queue.pop();
      //   memcpy(data, task.data, task.len);
      //   free(task.data);
      //   lck.unlock();
      // };

      // recv_threads_queue.push(std::thread(recv_func));

      // pending_recv_task_mutex.lock();
      printf(">>> pushed task len: %d\n", len);
      pending_recv_task_queue.push(recv_task{data, len});
      // pending_recv_task_mutex.unlock();

    }

    void recv_data_sync() {
      recv_task pending_task;
      while (!pending_recv_task_queue.empty()) {
        pending_task = pending_recv_task_queue.front();
        pending_recv_task_queue.pop();
        // printf("check recv_queue\n");
        std::unique_lock<std::mutex> lck(recv_queue_mutex);
        recv_queue_cv.wait(lck, [this] { return !recv_queue.empty(); });
        recv_task finished_task = recv_queue.front();
        recv_queue.pop();
        if (finished_task.len != pending_task.len) {
          printf("recv_data_sync: length mismatch, recv size: %d, pending size: %d", finished_task.len, pending_task.len);
          die("recv_data_sync: length mismatch");
        }
        memcpy(pending_task.data, finished_task.data, finished_task.len);
        if (pending_task.len == 4) {
          printf(">>> recv_data_sync: received a pointer, indicate length: %d\n", *(int *)pending_task.data);
          // printf(">>> at offset: %d\n", )
        } else {
          printf(">>> recv_data_sync: received raw data, length: %d\n", pending_task.len);
        }
        free(finished_task.data);
        // lck.unlock();
        // printf("recv finished\n");
      }
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

      for (uint i = 0; i < 4; i++)
        memcpy(&conn->send_msg[i].data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));
      send_message(conn, MSG_MR);
    }

    uint32_t num_sync() {
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
      printf("init_sndrecv.\n");
      // s_ctx->cq_poller_thread->join();
      poll_cq_connect(nullptr);
      printf("cq_poller_thread joined.\n");
      // delete s_ctx->cq_poller_thread;
      // s_ctx->cq_poller_thread = new std::thread([this]
                                                // { poll_cq_sndrecv(nullptr); });
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
      printf("post receive\n");
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
        printf("recved message, type: %d\n", conn->recv_msg->type);

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
          // printf("recv MSG_DATA\n");
          uint cur_recv_offset = 0;
          std::unique_lock<std::mutex> lk(recv_queue_mutex);
          for (uint i = 0; i < MAX_MESSAGE_NUM; i++) {
            uint size = conn->recv_msg->size[i];
            if (size == 0) {
              // printf("received %d messages\n", i);
              break;
            }
            void *data = malloc(size);
            // printf("cur_recv_offset: %d\n", cur_recv_offset);
            memcpy(data, (char *)conn->rdma_remote_buffer + cur_recv_offset, size);
            recv_task task(data, size);
            // printf("push task \n");
            recv_queue.push(task);
            cur_recv_offset += size;
          }
          lk.unlock();
          recv_queue_cv.notify_one();
          if (is_server) {
            post_receive_message(conn);
            std::unique_lock<std::mutex> sync_num_lk(sync_num_mutex);
            sync_num_cv.wait(sync_num_lk, [this] { return sync_num == finished_sync_num + 1; });
            // flush_start_cv.wait(flush_start_lk, [this] { return flush_start; });
            send_message(conn, MSG_DATA);
            sync_num_lk.unlock();
            // flush_start_lk.unlock();
          }
          else {
            send_message(conn, MSG_SYNC);
            std::unique_lock<std::mutex> lk(sync_num_mutex);
            finished_sync_num += 1;
            sync_num_cv.notify_one();
            lk.unlock();
            // posted_sync = false;
            // posted_sync = true;
          }
          // send_msg_mutex.unlock();
        }
        else if (conn->recv_msg->type == MSG_SYNC) { // only server will recv this
          // printf("recv MSG_SYNC\n");
          // std::unique_lock<std::mutex> lk(sync_done_mutex);
          // sync_done = true;
          // sync_done_cv.notify_one();
          // lk.unlock();
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
      }
      else if (wc->opcode == IBV_WC_SEND)
      { // send operation
        // printf("send completed\n");
        if (conn->send_state == SS_INIT)
        {
          conn->send_state = SS_MR_SENT;
          send_ready = true;
        }
        else if (conn->send_state == SS_MR_SENT)
        {
          // std::unique_lock<std::mutex> send_ready_lock(send_ready_mutex);
          // send_ready = true;
          // send_ready_lock.unlock();
          // send_ready_cv.notify_one();
          // printf("update completed msg num\n");
        }
        if (!is_server && posted_sync)
        {
          // std::unique_lock<std::mutex> lk(sync_done_mutex);
          // sync_done = true;
          // sync_done_cv.notify_one();
          // lk.unlock();
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
        // std::unique_lock<std::mutex> party_ready_lock(party_ready_mutex);
        // no need to wait
        party_ready = true;
        // party_ready_lock.unlock();
        // party_ready_cv.notify_one();
        initialized = true;

        // std::unique_lock<std::mutex> send_ready_lock(send_ready_mutex);
        // send_ready = true;
        // send_ready_lock.unlock();
        // send_ready_cv.notify_one();
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
      // printf("sync\n");
      // std::unique_lock<std::mutex> lk1(sync_num_mutex);
      sync_num += 1;
      printf("flush number: %d\n", sync_num);
      if (!is_server) {
        printf("send MSG_DATA\n");
        // post_receive_message(_conn_context);
        send_message(_conn_context, MSG_DATA);
      }
      poll_cq_sndrecv(nullptr);
      _conn_context->send_msg[1].size[0] = 0;
      cur_message_index = 0;
      cur_offset = 0;
      // lk1.unlock();
      // sync_num_cv.notify_one();
      
      // flush_start_mutex.lock();
      // flush_start = true;
      // flush_start_cv.notify_one();
      // std::unique_lock<std::mutex> lk2(sync_num_mutex);

      // sync_num_cv.wait(lk2, [this] { return sync_num == finished_sync_num; });
      // cur_offset = 0;
      // _conn_context->send_msg[1].size[0] = 0;
      // cur_message_index = 0;
      // lk2.unlock();
      // printf("pending recv threads\n");
      recv_data_sync();
      // printf("all recv threads done\n");
      // printf("sync finished\n");
      // flush_start = false;
      // flush_start_mutex.unlock();
    }

    void sync()
    { // ensure that all data is sent
      // printf("flush\n");
      // printf("send MSG_DATA\n");
      // send_message(_conn_context, MSG_DATA);
        // _conn_context->send_msg[1].size[0] = 0;
      // send_msg_mutex.unlock();
      // send again to make sure that all data is received and stored in buffer
      flush();
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
      // s_ctx->cq_poller_thread = new std::thread([this]()
                                                // { poll_cq_connect(NULL); });
    }

    void poll_cq_connect(void *ctx)
    {
      struct ibv_cq *cq;
      struct ibv_wc wc;

      while (1)
      {
        if (!cq_wc_queue.empty()) {
          die("I assume cq_wc_queue is empty");
        }
        printf("get cq event\n");
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        printf("ack cq event\n");
        ibv_ack_cq_events(cq, 1);
        printf("notify cq\n");
        TEST_NZ(ibv_req_notify_cq(cq, 0));
        printf("prepare to poll cq\n");

        int num_comp = ibv_poll_cq(cq, 1, &wc);
        while (num_comp && !party_ready)
        {
          // printf("num_comp: %d\n", num_comp);
          on_completion(&wc, is_server);
          printf("poll cq again, party ready: %d\n", party_ready);
          num_comp = ibv_poll_cq(cq, 1, &wc);
          // printf("num_comp: %d\n", num_comp);
        }

        if (num_comp) {
          cq_wc_queue.push(wc);
          while(ibv_poll_cq(cq, 1, &wc)) {
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

    void poll_cq_sndrecv(void *ctx)
    {
      struct ibv_cq *cur_cq;
      struct ibv_wc wc;
      sync_state _sync_state = SYNC_WAIT_MSG_DATA;
      while (_sync_state != SYNC_DONE)
      {
        if (!cq_wc_queue.empty()) {
          printf("deal with old wc\n");
          if (cq_wc_queue.size() > 1)
            die("I assume that there is only one wc in the queue");
          wc = cq_wc_queue.front();
          cq_wc_queue.pop();
          on_completion_sync(&wc, &_sync_state, is_server);
          // int num_comp = ibv_poll_cq(cur_cq, 1, &wc);

          // while (ibv_poll_cq(cur_cq, 1, &wc) && _sync_state != SYNC_DONE) {
            // printf("deal with new cq\n");
            // on_completion_sync(&wc, &_sync_state, is_server);
          // }
        }
        if (_sync_state == SYNC_DONE)
          break;
        printf("get cq event\n");
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cur_cq, &ctx));
        printf("ack event\n");
        ibv_ack_cq_events(cur_cq, 1);
        printf("notify cq\n");
        TEST_NZ(ibv_req_notify_cq(cur_cq, 0));
        printf("poll_cq\n");
        
        int num_comp = ibv_poll_cq(cur_cq, 1, &wc);
        printf("out: num_comp: %d, state: %d\n", num_comp, _sync_state);
        while (num_comp && _sync_state != SYNC_DONE)
        {
          on_completion_sync(&wc, &_sync_state, is_server);
          printf("poll cq again\n");
          num_comp = ibv_poll_cq(cur_cq, 1, &wc);
          printf("num_comp: %d, state: %d\n", num_comp, _sync_state);
        }
        if (num_comp != 0)
        {
          cq_wc_queue.push(wc);
          while(ibv_poll_cq(cur_cq, 1, &wc)) {
            cq_wc_queue.push(wc);
          }
        }
        // // printf("get cq event\n");
        // if (_sync_state != SYNC_DONE) {
        //   TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cur_cq, &ctx));
        //   ibv_ack_cq_events(cur_cq, 1);
        //   TEST_NZ(ibv_req_notify_cq(cur_cq, 0));
        //   num_comp = ibv_poll_cq(cur_cq, 1, &wc);
        //   // printf("poll cq done\n");
        //   while (num_comp && _sync_state != SYNC_DONE)
        //   {
        //     on_completion_sync(&wc, &_sync_state, is_server);
        //     // printf("poll cq again\n");
        //     num_comp = ibv_poll_cq(cur_cq, 1, &wc);
        //     // printf("num_comp: %d\n", num_comp);
        //   }
        // }

        if (party_done)
          break;
      }
    }
    // void poll_cq_sndrecv(void *ctx)
    // {
    //   // struct ibv_cq *cq;
    //   struct ibv_wc wc;
    //   sync_state _sync_state = SYNC_WAIT_MSG_DATA;
    //   while (_sync_state != SYNC_DONE)
    //   {
    //     if (cur_cq == nullptr) {
    //       TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cur_cq, &ctx));
    //       ibv_ack_cq_events(cur_cq, 1);
    //       TEST_NZ(ibv_req_notify_cq(cur_cq, 0));
    //     }
    //     int num_comp = ibv_poll_cq(cur_cq, 1, &wc);

    //     while (num_comp && _sync_state != SYNC_DONE)
    //     {
    //       on_completion_sync(&wc, &_sync_state, is_server);
    //       // printf("poll cq again\n");
    //       num_comp = ibv_poll_cq(cur_cq, 1, &wc);
    //       // printf("num_comp: %d\n", num_comp);
    //     }
    //     // printf("get cq event\n");
    //     if (_sync_state != SYNC_DONE) {
    //       TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cur_cq, &ctx));
    //       ibv_ack_cq_events(cur_cq, 1);
    //       TEST_NZ(ibv_req_notify_cq(cur_cq, 0));
    //       num_comp = ibv_poll_cq(cur_cq, 1, &wc);
    //       // printf("poll cq done\n");
    //       while (num_comp && _sync_state != SYNC_DONE)
    //       {
    //         on_completion_sync(&wc, &_sync_state, is_server);
    //         // printf("poll cq again\n");
    //         num_comp = ibv_poll_cq(cur_cq, 1, &wc);
    //         // printf("num_comp: %d\n", num_comp);
    //       }
    //     }

    //     if (party_done)
    //       break;
    //   }
    // }

    void on_completion_sync(struct ibv_wc *wc, sync_state *state, bool is_server) {
      if (wc->status != IBV_WC_SUCCESS) {
        printf("wc->status: %d\n", wc->status);
        die("on_completion: wc->status is not IBV_WC_SUCCESS");
      }

      if (wc->opcode == IBV_WC_RECV) {
        struct conn_context *conn = (struct conn_context *)wc->wr_id;
        message *msg = conn->recv_msg;
        if (msg->type == MSG_DATA && *state == SYNC_WAIT_MSG_DATA) {
          printf("recv MSG_DATA\n");
          // fetch data
          uint cur_recv_offset = 0;
          for (uint i = 0; i < MAX_MESSAGE_NUM; i++) {
            uint size = conn->recv_msg->size[i];
            if (size == 0) {
              // printf("received %d messages\n", i);
              break;
            }
            void *data = malloc(size);
            printf("cur_recv_offset: %d\n", cur_recv_offset);
            
            memcpy(data, (char *)conn->rdma_remote_buffer + cur_recv_offset, size);
            if (size == 4) {
              printf("get pointer data from remote, offset: %d size: %d\n", cur_recv_offset, *(int *) ((char *)conn->rdma_remote_buffer + cur_recv_offset));
            } else {
              printf("get data block from offset: %d, length: %d\n", cur_recv_offset, size);
            }
            recv_task task(data, size);
            // printf("push task \n");
            recv_queue.push(task);
            cur_recv_offset += size;
          }
          
          post_receive_message(conn);
          if (is_server) {
            // *state = SYNC_WAIT_SEND_DONE;
            *state = SYNC_WAIT_MSG_SYNC;
            printf("pending rdma wirte: %d\n", pending_rdma_write);
            // printf("send MSG_DATA\n");
            send_message(conn, MSG_DATA);
          } else {
            // printf("send MSG_SYNC\n");
            send_message(conn, MSG_SYNC);
            *state = SYNC_WAIT_MSG_SYNC;
          }
        } else if (msg->type == MSG_SYNC && *state == SYNC_WAIT_MSG_SYNC) {
          post_receive_message(conn);
          if (is_server) {
            // printf("send MSG_SYNC\n");
            send_message(conn, MSG_SYNC);
            *state = SYNC_WAIT_SEND_DONE;
          } else {
            *state = SYNC_DONE;
          }
        } else {
          printf("error: unexpected message type: %d, state: %d\n", msg->type, *state);
          die("on_completion: message type and state does not match, should be WAIT_MSG_DATA");
        }
      } else if (wc->opcode == IBV_WC_SEND) {
        // printf("send message done\n");
        if (*state == SYNC_WAIT_SEND_DONE) {
          *state = SYNC_DONE;
        }
      } else if (wc->opcode == IBV_WC_RDMA_WRITE) {
        pending_rdma_write -= 1;
        // printf("rdma write done\n");
        // if (is_server && pending_rdma_write == 0) {

        // } else if (!is_server && pending_rdma_write == 0) {

        // }
      } else {
        printf("error: unexpected opcode: %d\n", wc->opcode);
        die("on_completion: opcode is not IBV_WC_SEND or IBV_WC_RECV");
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

      // printf("send message MSG type: %d, cur_message_num: %d, length: %d\n", type, cur_message_index, conn->send_msg[1].size[0]);
      
      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
    }

    void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
    {
      memset(qp_attr, 0, sizeof(*qp_attr));

      qp_attr->send_cq = s_ctx->cq;
      qp_attr->recv_cq = s_ctx->cq;
      qp_attr->qp_type = IBV_QPT_RC;

      qp_attr->cap.max_send_wr = 100;
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

      // params->initiator_depth = params->responder_resources = 1;
      // params->rnr_retry_count = 7; /* infinite retry */
    }
  };

}

#endif // NETWORK_IO_CHANNEL
