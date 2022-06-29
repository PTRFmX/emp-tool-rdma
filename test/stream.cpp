#include <sstream>
#include <iostream>
#include <thread>
#include <string>
#include <mutex>
#include <chrono>
#include <memory.h>
#include <stdio.h>


class StreamBuffer {
public:
  StreamBuffer(size_t size) {
    buffer = malloc(size);
    buffer_len = size;
    available_len = size;
  }
  
  ~StreamBuffer() {
    free(buffer);
  }
  
  size_t write(const void *data, size_t len) {
    if (available_len < len) {
      printf("buffer space not enough\n");
      return 0;
    }
    
    if (ppos + len > buffer_len) {
      uint64_t first_block_size = buffer_len - ppos;
      uint64_t second_block_size = len - first_block_size;
      memcpy((char *)buffer + ppos, data, first_block_size);
      memcpy((char *)buffer, data, second_block_size);
      ppos = second_block_size;
    } else {
      memcpy((char *)buffer + ppos, data, len);
      ppos += len;
      ppos = ppos % buffer_len; // in case ppos is after the last one.
    }
    write_size_total += len;
    available_len -= len; 
    return len;
  }

  size_t read(void *dst, size_t len) {
    if (data_len() < len) {
      printf("error, cannot read that much data, available len: %ld\n", data_len());
    }
    
    if (gpos + len > buffer_len) {
      uint64_t first_block_size = buffer_len - gpos;
      uint64_t second_block_size = len - first_block_size;
    //   memcpy((char *)buffer + ppos, data, first_block_size);
    //   memcpy((char *)buffer, data, second_block_size);
      memcpy(dst, (char *)buffer + gpos, first_block_size);
      memcpy(dst + first_block_size, buffer, second_block_size);
      gpos = second_block_size;
    } else {
    //   memcpy((char *)buffer + ppos, data, len);
      memcpy(dst, (char *)buffer + gpos, len);
      gpos += len;
      gpos = gpos % buffer_len; // in case ppos is after the last one.
    }
    read_size_total += len;
    available_len += len; 
    return len;
  }
  
  uint64_t data_len() { // return the size of data can be read, in bytes.
    if (write_size_total < read_size_total) {
      printf("error, write is less than read\n");
      return 0;
    }
    return write_size_total - read_size_total;
  }
  
  
  uint64_t available_len = 0; // data that can be write, in bytes
  uint64_t gpos = 0; // read pointer
  uint64_t ppos = 0; // write pointer
  uint64_t write_size_total = 0; // total data writen, in bytes
  uint64_t read_size_total = 0; // total data read, in bytes
  void *buffer;
  size_t buffer_len;
};

std::mutex ss_mutex;

void write_ss(StreamBuffer *sb) {
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  ss_mutex.lock();
  std::cout << "write ss\n";
  std::string s("test ss");
  sb->write(s.c_str(), s.size());

  ss_mutex.unlock();
}

int main() {
  StreamBuffer sb(100);
  std::string s("test");
  std::thread t(write_ss, &sb);
  char *buffer = new char[100];

  uint32_t read_size = 0;
  while (1) {
    ss_mutex.lock();
    if (sb.data_len() < s.size()) {
      ss_mutex.unlock();
      continue;
    } else
      break;
  }
  sb.read(buffer, s.size());
  ss_mutex.unlock();
  t.join();
  std::cout << buffer;
//   uint32_t cnt = 0;
//   char *buffer = new char[100];
//   for (uint i = 0; i < 10000; i++) {
//   ss.read(buffer, s.size());
//   if (ss)
//     std::cout << buffer;
//   else {
//     cnt += ss.gcount();
//     ss.clear();
//   }
//   }
//   ss.write(s.c_str(), s.size());
//   ss.read(buffer, s.size());
//   std::cout << buffer;

  delete []buffer;


}