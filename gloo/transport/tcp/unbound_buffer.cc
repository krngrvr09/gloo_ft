/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "gloo/transport/tcp/unbound_buffer.h"
#include<thread>
#include <stdexcept>
#include <iostream>
#include "gloo/common/error.h"
#include "gloo/common/logging.h"
#include "gloo/transport/tcp/context.h"

namespace gloo {
namespace transport {
namespace tcp {

UnboundBuffer::UnboundBuffer(
    const std::shared_ptr<Context>& context,
    void* ptr,
    size_t size)
    : ::gloo::transport::UnboundBuffer(ptr, size),
      context_(context),
      recvCompletions_(0),
      recvRank_(-1),
      sendCompletions_(0),
      sendRank_(-1),
      shareableNonOwningPtr_(this) {}

UnboundBuffer::~UnboundBuffer() {}

void UnboundBuffer::handleRecvCompletion(int rank) {
  // std::cout<<"UnboundBuffer:handleRecvCompletion start"<<std::endl;
  std::lock_guard<std::mutex> lock(m_);
  recvCompletions_++;
  recvRank_ = rank;
  recvCv_.notify_one();
  // std::cout<<"UnboundBuffer:handleRecvCompletion end"<<std::endl;
}

void UnboundBuffer::abortWaitRecv() {
  std::lock_guard<std::mutex> guard(m_);
  abortWaitRecv_ = true;
  recvCv_.notify_one();
}

void UnboundBuffer::abortWaitSend() {
  std::lock_guard<std::mutex> guard(m_);
  abortWaitSend_ = true;
  sendCv_.notify_one();
}

bool UnboundBuffer::waitRecv(int* rank, std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(m_);
  if (timeout == kUnsetTimeout) {
    timeout = context_->getTimeout();
  }

  if (recvCompletions_ == 0) {
    auto done = recvCv_.wait_for(lock, timeout, [&] {
      throwIfException();
      return abortWaitRecv_ || recvCompletions_ > 0;
    });
    if (!done) {
      // Below, we let all pairs in the transport context know about this
      // application side timeout. This in turn will call into all pending
      // operations to let them know about the error. This includes the
      // operation that is pending for this buffer, so in order for a call to
      // this instance its 'signalException' function to not deadlock, we need
      // to first release the instance wide lock.
      lock.unlock();

      // Signal all pairs about this application timeout.
      // Note that the exception that they see indicates it was another
      // operation that timed out. This this exception surfaces anywhere,n
      // be sure to look for the actual cause (seen below).
      context_->signalException("Application timeout caused pair closure");

      throw ::gloo::IoException(
              GLOO_ERROR_MSG(
                  "Timed out waiting ",
                  timeout.count(),
                  "ms for recv operation to complete"));
    }
  }
  if (abortWaitRecv_) {
    // Reset to false, so that only this waitRecv is interrupted
    abortWaitRecv_ = false;
    return false;
  }
  recvCompletions_--;
  if (rank != nullptr) {
    *rank = recvRank_;
  }
  return true;
}

void UnboundBuffer::handleSendCompletion(int rank) {
  // std::cout<<"UnboundBuffer::handleSendCompletion start for rank: "<<rank<<std::endl;
  std::lock_guard<std::mutex> lock(m_);
  sendCompletions_++;
  sendRank_ = rank;
  sendCv_.notify_one();
  // std::cout<<"UnboundBuffer::handleSendCompletion end for rank: "<<rank<<std::endl;
}

bool UnboundBuffer::waitSend(int* rank, std::chrono::milliseconds timeout) {
  // std::cout<<"UnBoundBuffer:waitSend start"<<std::endl;
  //std::cout<<"UnBoundBuffer:waitSend rank: "<<(*rank)<<std::endl;
  std::unique_lock<std::mutex> lock(m_);
  if (timeout == kUnsetTimeout) {
    timeout = context_->getTimeout();
  }

  if (sendCompletions_ == 0) {
    // std::cout<<"UnBoundBuffer:waitSend about to call sendCv->wait_for"<<std::endl;
    auto done = sendCv_.wait_for(lock, timeout, [&] {
        std::thread::id this_id = std::this_thread::get_id();
        // std::cout<<"UnboundBuffer:waitSend inside predicate, thread: "<<this_id<<std::endl;
        throwIfException();
        if(abortWaitSend_){
          // std::cout<<"UnboundBuffer:waitSend abortWaitSend_ is true"<<std::endl;
        }
        // std::cout<<"UnboundBuffer:waitSend sendCompletions_"<<sendCompletions_<<std::endl;
        return abortWaitSend_ || sendCompletions_ > 0;
      });
    if (!done) {
      // std::cout<<"UnBoundBuffer:waitSend not done"<<std::endl;
      // Below, we let all pairs in the transport context know about this
      // application side timeout. This in turn will call into all pending
      // operations to let them know about the error. This includes the
      // operation that is pending for this buffer, so in order for a call to
      // this instance its 'signalException' function to not deadlock, we need
      // to first release the instance wide lock.
      lock.unlock();

      // Signal all pairs about this application timeout.
      // Note that the exception that they see indicates it was another
      // operation that timed out. This this exception surfaces anywhere,n
      // be sure to look for the actual cause (seen below).
      context_->signalException("Application timeout caused pair closure");

      throw ::gloo::IoException(
          GLOO_ERROR_MSG(
              "Timed out waiting ",
              timeout.count(),
              "ms for send operation to complete"));
    }
    
  }
  // std::cout<<"UnBoundBuffer:waitSend 1"<<std::endl;
  if (abortWaitSend_) {
    // std::cout<<"UnBoundBuffer:waitSend 2"<<std::endl;
    // Reset to false, so that only this waitSend is interrupted
    abortWaitSend_ = false;
    return false;
  }
  // std::cout<<"UnBoundBuffer:waitSend 3"<<std::endl;
  sendCompletions_--;
  if (rank != nullptr) {
    // std::cout<<"UnBoundBuffer:waitSend 4"<<std::endl;
    *rank = sendRank_;
  }
  // std::cout<<"UnBoundBuffer:waitSend end"<<std::endl;
  return true;
}

void UnboundBuffer::send(
    int dstRank,
    uint64_t slot,
    size_t offset,
    size_t nbytes) {
      // std::cout<<"UnboundBuffer: send start"<<std::endl;
  // Default the number of bytes to be equal to the number
  // of bytes remaining in the buffer w.r.t. the offset.
  if (nbytes == kUnspecifiedByteCount) {
    GLOO_ENFORCE_LE(offset, this->size);
    nbytes = this->size - offset;
  }
  context_->getPair(dstRank)->send(this, slot, offset, nbytes);
  // std::cout<<"UnboundBuffer: send end"<<std::endl;
}

void UnboundBuffer::recv(
    int srcRank,
    uint64_t slot,
    size_t offset,
    size_t nbytes) {
      // std::cout<<"unbound_buffer recv start"<<std::endl;
  // Default the number of bytes to be equal to the number
  // of bytes remaining in the buffer w.r.t. the offset.
  if (nbytes == kUnspecifiedByteCount) {
    GLOO_ENFORCE_LE(offset, this->size);
    nbytes = this->size - offset;
  }
    // std::cout<<"pair recv 1"<<std::endl;
  context_->getPair(srcRank)->recv(this, slot, offset, nbytes);
    // std::cout<<"pair recv 2"<<std::endl;
}

void UnboundBuffer::destroy(int srcRank) {
  // std::cout<<"unbound_buffer DESTROY start"<<std::endl;
  if(context_ == nullptr) {
    // std::cout<<"unbound_buffer::NPE1"<<std::endl;
  }
  if(context_->getPair(srcRank) == nullptr) {
    // std::cout<<"unbound_buffer::NPE2"<<std::endl;
  }
  context_->getPair(srcRank)->destroy();
  // std::cout<<"unbound_buffer DESTROY end"<<std::endl;
}

void UnboundBuffer::recv(
    std::vector<int> srcRanks,
    uint64_t slot,
    size_t offset,
    size_t nbytes) {
  // Default the number of bytes to be equal to the number
  // of bytes remaining in the buffer w.r.t. the offset.
  if (nbytes == kUnspecifiedByteCount) {
    GLOO_ENFORCE_LT(offset, this->size);
    nbytes = this->size - offset;
  }
  context_->recvFromAny(this, slot, offset, nbytes, srcRanks);
}

void UnboundBuffer::signalException(std::exception_ptr ex) {
  std::lock_guard<std::mutex> lock(m_);
  ex_ = std::move(ex);
  recvCv_.notify_all();
  sendCv_.notify_all();
}

void UnboundBuffer::throwIfException() {
  if (ex_ != nullptr) {
    // std::cout<<"UnboundBuffer:throwingIfException"<<std::endl;
    std::rethrow_exception(ex_);
  }
}

} // namespace tcp
} // namespace transport
} // namespace gloo
