// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "linux_aligned_file_reader.h"

#include "coroutine_execute_io.cpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/coroutine2/all.hpp>
#include <cassert>
#include <cstdio>
#include <iostream>
#include <liburing.h>
#include <coroutine>
#include "tsl/robin_map.h"
#include "utils.h"
#include <future>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <libaio.h>
#include <chrono>
#define MAX_EVENTS 1024


// 定义协程任务
// struct Task {
//     struct promise_type;
//     using handle_type = std::coroutine_handle<promise_type>;

//     handle_type coro;

//     Task(handle_type h) : coro(h) {}
//     ~Task() {
//         if (coro) coro.destroy();
//     }
//     bool await_ready() const noexcept { return false; }
//     void await_suspend(std::coroutine_handle<>) const noexcept {}
//     void await_resume() const noexcept {}

//     struct promise_type {
//         auto get_return_object() { return Task{handle_type::from_promise(*this)}; }
//         std::suspend_never initial_suspend() { return {}; }
//         std::suspend_always final_suspend() noexcept { return {}; }
//         void return_void() {}
//         void unhandled_exception() { std::terminate(); }
//     };
// };

// // 异步 I/O 操作的协程函数
// Task async_read(io_context_t ctx, int fd, AlignedRead& read_req) {
//     struct iocb cb;
//     struct iocb* cbs[1] = { &cb };
//     io_prep_pread(&cb, fd, read_req.buf, read_req.len, read_req.offset);

//     int ret = io_submit(ctx, 1, cbs);
//     if (ret < 0) {
//         std::cerr << "io_submit failed: " << strerror(-ret) << "\n";
//         co_return;
//     }

//     // 挂起协程，等待 I/O 完成
//     struct io_event evt;
//     ret = io_getevents(ctx, 1, 1, &evt, nullptr);
//     if (ret < 0) {
//         std::cerr << "io_getevents failed: " << strerror(-ret) << "\n";
//     }
//     co_return;
// }

// // 协程驱动的批量 I/O 函数
// Task coroutine_execute_io(io_context_t ctx, int fd, std::vector<AlignedRead>& read_reqs) {
//     for (auto& req : read_reqs) {
//         co_await async_read(ctx, fd, req);
//     }
//     co_return;
// }


// using namespace boost::coroutines2;
// void co_push_fun1(coroutine<int>::push_type& yield,int a){
//     for(int i = 0; i < a;i++){
//         // logic before
//     }
// }

namespace
{
typedef struct io_event io_event_t;
typedef struct iocb iocb_t;




void async_execute_io(io_context_t ctx, int fd, std::vector<AlignedRead> &read_reqs, uint64_t n_retries = 0) {
    std::vector<std::future<void>> futures;
    
    for (auto &req : read_reqs) {
        futures.emplace_back(std::async(std::launch::async, [&, fd, req]() {
            // 尝试多次以应对读取失败
            for (uint64_t attempt = 0; attempt <= n_retries; ++attempt) {
                if (lseek(fd, req.offset, SEEK_SET) == -1) {
                    std::cerr << "Error seeking in file: " << strerror(errno) << std::endl;
                    return;
                }
                
                ssize_t bytesRead = read(fd, req.buf, req.len);
                if (bytesRead == -1) {
                    if (attempt == n_retries) {
                        std::cerr << "Error reading file after " << n_retries << " retries: " << strerror(errno) << std::endl;
                        return;
                    }
                } else {
                    // 成功读取后退出重试循环
                    break;
                }
            }
        }));
    }

    // 等待所有 I/O 操作完成
    for (auto &fut : futures) {
        fut.get();
    }
}

void execute_io(io_context_t ctx, int fd, std::vector<AlignedRead> &read_reqs, uint64_t n_retries = 0)
{
#ifdef DEBUG
    for (auto &req : read_reqs)
    {
        assert(IS_ALIGNED(req.len, 512));
        // std::cout << "request:"<<req.offset<<":"<<req.len << std::endl;
        assert(IS_ALIGNED(req.offset, 512));
        assert(IS_ALIGNED(req.buf, 512));
        // assert(malloc_usable_size(req.buf) >= req.len);
    }
#endif

    // break-up requests into chunks of size MAX_EVENTS each
    uint64_t n_iters = ROUND_UP(read_reqs.size(), MAX_EVENTS) / MAX_EVENTS;
    if(n_iters >1)std::cout<<"n_iters >1, = "<<n_iters<<std::endl;
    for (uint64_t iter = 0; iter < n_iters; iter++)
    {
        uint64_t n_ops = std::min((uint64_t)read_reqs.size() - (iter * MAX_EVENTS), (uint64_t)MAX_EVENTS);
        std::vector<iocb_t *> cbs(n_ops, nullptr);
        std::vector<io_event_t> evts(n_ops);
        std::vector<struct iocb> cb(n_ops);
        for (uint64_t j = 0; j < n_ops; j++)
        {
            io_prep_pread(cb.data() + j, fd, read_reqs[j + iter * MAX_EVENTS].buf, read_reqs[j + iter * MAX_EVENTS].len,
                          read_reqs[j + iter * MAX_EVENTS].offset);
        }

        // initialize `cbs` using `cb` array
        //

        for (uint64_t i = 0; i < n_ops; i++)
        {
            cbs[i] = cb.data() + i;
        }

        uint64_t n_tries = 0;
        // No retries in default.
        while (n_tries <= n_retries)
        {
            // issue each iter reads
            // iter size = 1024 in default
            int64_t ret = io_submit(ctx, (int64_t)n_ops, cbs.data());
            // if requests didn't get accepted
            if (ret != (int64_t)n_ops)
            {
                std::cerr << "io_submit() failed; returned " << ret << ", expected=" << n_ops << ", ernno=" << errno
                          << "=" << ::strerror(-ret) << ", try #" << n_tries + 1;
                std::cout << "ctx: " << ctx << "\n";
                exit(-1);
            }
            else
            {
                // wait on io_getevents
                ret = io_getevents(ctx, (int64_t)n_ops, (int64_t)n_ops, evts.data(), nullptr);
                // if requests didn't complete
                if (ret != (int64_t)n_ops)
                {
                    std::cerr << "io_getevents() failed; returned " << ret << ", expected=" << n_ops
                              << ", ernno=" << errno << "=" << ::strerror(-ret) << ", try #" << n_tries + 1;
                    exit(-1);
                }
                else
                {
                    break;
                }
            }
        }
        // disabled since req.buf could be an offset into another buf
        /*
        for (auto &req : read_reqs) {
          // corruption check
          assert(malloc_usable_size(req.buf) >= req.len);
        }
        */
    }
}
} // namespace

LinuxAlignedFileReader::LinuxAlignedFileReader()
{
    this->file_desc = -1;
}

LinuxAlignedFileReader::~LinuxAlignedFileReader()
{
    int64_t ret;
    // check to make sure file_desc is closed
    ret = ::fcntl(this->file_desc, F_GETFD);
    if (ret == -1)
    {
        if (errno != EBADF)
        {
            std::cerr << "close() not called" << std::endl;
            // close file desc
            ret = ::close(this->file_desc);
            // error checks
            if (ret == -1)
            {
                std::cerr << "close() failed; returned " << ret << ", errno=" << errno << ":" << ::strerror(errno)
                          << std::endl;
            }
        }
    }
}

io_context_t &LinuxAlignedFileReader::get_ctx()
{
    std::unique_lock<std::mutex> lk(ctx_mut);
    // perform checks only in DEBUG mode
    if (ctx_map.find(std::this_thread::get_id()) == ctx_map.end())
    {
        std::cerr << "bad thread access; returning -1 as io_context_t" << std::endl;
        return this->bad_ctx;
    }
    else
    {
        return ctx_map[std::this_thread::get_id()];
    }
}

void LinuxAlignedFileReader::register_thread()
{
    auto my_id = std::this_thread::get_id();
    std::unique_lock<std::mutex> lk(ctx_mut);
    if (ctx_map.find(my_id) != ctx_map.end())
    {
        std::cerr << "multiple calls to register_thread from the same thread" << std::endl;
        return;
    }
    io_context_t ctx = 0;
    int ret = io_setup(MAX_EVENTS, &ctx);
    if (ret != 0)
    {
        lk.unlock();
        if (ret == -EAGAIN)
        {
            std::cerr << "io_setup() failed with EAGAIN: Consider increasing /proc/sys/fs/aio-max-nr" << std::endl;
        }
        else
        {
            std::cerr << "io_setup() failed; returned " << ret << ": " << ::strerror(-ret) << std::endl;
        }
    }
    else
    {
        diskann::cout << "allocating ctx: " << ctx << " to thread-id:" << my_id << std::endl;
        ctx_map[my_id] = ctx;
    }
    lk.unlock();
}

void LinuxAlignedFileReader::deregister_thread()
{
    auto my_id = std::this_thread::get_id();
    std::unique_lock<std::mutex> lk(ctx_mut);
    assert(ctx_map.find(my_id) != ctx_map.end());

    lk.unlock();
    io_context_t ctx = this->get_ctx();
    io_destroy(ctx);
    //  assert(ret == 0);
    lk.lock();
    ctx_map.erase(my_id);
    std::cerr << "returned ctx from thread-id:" << my_id << std::endl;
    lk.unlock();
}

void LinuxAlignedFileReader::deregister_all_threads()
{
    std::unique_lock<std::mutex> lk(ctx_mut);
    for (auto x = ctx_map.begin(); x != ctx_map.end(); x++)
    {
        io_context_t ctx = x.value();
        io_destroy(ctx);
        //  assert(ret == 0);
        //  lk.lock();
        //  ctx_map.erase(my_id);
        //  std::cerr << "returned ctx from thread-id:" << my_id << std::endl;
    }
    ctx_map.clear();
    //  lk.unlock();
}

void LinuxAlignedFileReader::open(const std::string &fname)
{
    int flags = O_DIRECT | O_RDONLY | O_LARGEFILE;
    this->file_desc = ::open(fname.c_str(), flags);
    // error checks
    assert(this->file_desc != -1);
    std::cerr << "Opened file : " << fname << std::endl;
}

void LinuxAlignedFileReader::close()
{
    //  int64_t ret;

    // check to make sure file_desc is closed
    ::fcntl(this->file_desc, F_GETFD);
    //  assert(ret != -1);

    ::close(this->file_desc);
    //  assert(ret != -1);
}

void LinuxAlignedFileReader::read(std::vector<AlignedRead> &read_reqs, io_context_t &ctx, bool async)
{
    // if (async == true)
    // {
    //     diskann::cout << "Async currently not supported in linux." << std::endl;
    // }
    assert(this->file_desc != -1);

    if(!async){
        execute_io(ctx, this->file_desc, read_reqs);
    }else{
        BQANN::coro_execute_IO(this->file_desc, read_reqs);
    }

}