#include <cassert>
#include <cstdio>
#include <iostream>
#include <coroutine>
#include <liburing.h>
#include "aligned_file_reader.h"
#include "tsl/robin_map.h"
#include "utils.h"
#define MAX_EVENTS 1024


namespace BQANN

{

class IOUring {
public:
  explicit IOUring(size_t queue_size) {
    if (auto s = io_uring_queue_init(queue_size, &ring_, 0); s < 0) {
      throw std::runtime_error("error initializing io_uring: " + std::to_string(s));
    }
  }

  IOUring(const IOUring &) = delete;
  IOUring &operator=(const IOUring &) = delete;
  IOUring(IOUring &&) = delete;
  IOUring &operator=(IOUring &&) = delete;

  ~IOUring() { io_uring_queue_exit(&ring_); }

  struct io_uring *get() {
    return &ring_;
  }

private:
  struct io_uring ring_;
};

struct CoroRequestData {
  std::coroutine_handle<> handle;
  int statusCode{-1};
};


class AlignedReadAwaitable {
public:
  AlignedReadAwaitable(IOUring &uring, int fd, const AlignedRead &aligned_read) {
    sqe_ = io_uring_get_sqe(uring.get());
    io_uring_prep_read(sqe_, fd, aligned_read.buf, aligned_read.len, aligned_read.offset);
  }

  auto operator co_await() {
    struct Awaiter {
      io_uring_sqe *entry;
      CoroRequestData requestData;

      Awaiter(io_uring_sqe *sqe) : entry{sqe} {}
      bool await_ready() { return false; }
      void await_suspend(std::coroutine_handle<> handle) noexcept {
        requestData.handle = handle;
        io_uring_sqe_set_data(entry, &requestData);
      }
      int await_resume() { return requestData.statusCode; }
    };
    return Awaiter{sqe_};
  }

private:
  io_uring_sqe *sqe_;
};

// 检查cqe队列中返回的io能够正确处理
int consumeCQEntries(IOUring &uring) {
  int processed{0};
  io_uring_cqe *cqe;
  unsigned head; // head of the ring buffer, unused
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    auto *request_data = static_cast<CoroRequestData *>(io_uring_cqe_get_data(cqe));
    // make sure to set the status code before resuming the coroutine
    request_data->statusCode = cqe->res;
    request_data->handle.resume(); // await_resume is called here
    ++processed;
  }
  io_uring_cq_advance(uring.get(), processed);
  return processed;
}

// submits pending submission entries to the kernel and blocks until at least one completion entry arrives:
int consumeCQEntriesBlocking(IOUring &uring) {
  io_uring_submit_and_wait(uring.get(), 1); // blocks if queue empty
  return consumeCQEntries(uring);
}

struct Result {
  // tinyobj::ObjReader result; // stores the actual parsed obj
  int status_code{0};        // the status code of the read operation
  std::string file;          // the file the OBJ was loaded from
};


class Task {
public:
  struct promise_type {
    Result result;

    Task get_return_object() { return Task(this); }

    void unhandled_exception() noexcept {}

    void return_value(Result result) noexcept { result = std::move(result); }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
  };

  explicit Task(promise_type *promise)
      : handle_{HandleT::from_promise(*promise)} {}
  Task(Task &&other) : handle_{std::exchange(other.handle_, nullptr)} {}

  ~Task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  Result getResult() const & {
    assert(handle_.done());
    return handle_.promise().result;
  }

  Result&& getResult() && {
    assert(handle_.done());
    return std::move(handle_.promise().result);
  }

  bool done() const { return handle_.done(); }

  using HandleT = std::coroutine_handle<promise_type>;
  HandleT handle_;
};


Task readAlignedRead(IOUring &uring,int fd, const AlignedRead &req) {
  std::vector<char> buff(req.len);
  int status = co_await AlignedReadAwaitable{uring, fd, req};
  // completion entry has arrived at this point
  // buffer has been filled and parsing can take place
  Result result{.status_code = 0, .file = ""};
  // readObjFromBuffer(buff, result.result);
  co_return result;
}

bool allDone(const std::vector<Task> &tasks) {
  return std::all_of(tasks.cbegin(), tasks.cend(),
                     [](const auto &t) { return t.done(); });
}

std::vector<Result> gatherResults(const std::vector<Task> &tasks) {
  std::vector<Result> results;
  results.reserve(tasks.size());
  for (auto &&t : tasks) {
    results.push_back(std::move(t).getResult());
  }
  return results;
}

void coro_execute_IO(int fd, const std::vector<AlignedRead> &read_reqs) {
  IOUring uring{read_reqs.size()};
  std::vector<Task> tasks;
  tasks.reserve(read_reqs.size());
  for (const auto &read_req : read_reqs) {
    tasks.push_back(readAlignedRead(uring,fd,read_req));
  }
  while (!allDone(tasks)) {
    // consume all entries in the submission queue
    // if the queue is empty block until the next completion arrives
    consumeCQEntriesBlocking(uring);
  }
  std::vector<Result> tmp = gatherResults(tasks);
  return ;
}




}// namespace BQANN
