// Copyright (c) 2014 Baidu, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: wangxuefeng (wangxuefeng@didichuxing.com)

#ifndef BRPC_THRIFT_MESSAGE_H
#define BRPC_THRIFT_MESSAGE_H

#include <functional>
#include <string>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_message_reflection.h>
#include "google/protobuf/descriptor.pb.h"

#include "butil/iobuf.h"
#include "butil/class_name.h"
#include "brpc/channel_base.h"
#include "brpc/controller.h"

#include <thrift/Thrift.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

namespace apache {
namespace thrift {
class TBase;
namespace protocol {
class TProtocol;
}
}
}

// _THRIFT_STDCXX_H_ is defined by thrift/stdcxx.h which was added since thrift 0.11.0
#include <thrift/TProcessor.h> // to include stdcxx.h if present
#ifndef THRIFT_STDCXX
 #if defined(_THRIFT_STDCXX_H_)
 # define THRIFT_STDCXX apache::thrift::stdcxx
 #else
 # define THRIFT_STDCXX boost
 # include <boost/make_shared.hpp>
 #endif
#endif

namespace brpc {

// Internal implementation detail -- do not call these.
void protobuf_AddDesc_baidu_2frpc_2fthrift_framed_5fmessage_2eproto();
void protobuf_AssignDesc_baidu_2frpc_2fthrift_framed_5fmessage_2eproto();
void protobuf_ShutdownFile_baidu_2frpc_2fthrift_framed_5fmessage_2eproto();

class ThriftStub;

static const int16_t THRIFT_INVALID_FID = -1;
static const int16_t THRIFT_REQUEST_FID = 1;
static const int16_t THRIFT_RESPONSE_FID = 0;

// Problem: TBase is absent in thrift 0.9.3
// Solution: Wrap native messages with templates into instances inheriting
//   from ThriftMessageBase which can be stored and handled uniformly.
class ThriftMessageBase {
public:
    virtual ~ThriftMessageBase() {};
    virtual uint32_t Read(::apache::thrift::protocol::TProtocol* iprot) = 0;
    virtual uint32_t Write(::apache::thrift::protocol::TProtocol* oprot) const = 0;
};

// Representing a thrift framed request or response.
class ThriftFramedMessage : public ::google::protobuf::Message {
friend class ThriftStub;
public:
    butil::IOBuf body; // ~= "{ raw_instance }"
    int16_t field_id;  // must be set when body is set.
    
private:
    bool _own_raw_instance;
    ThriftMessageBase* _raw_instance;

public:
    ThriftMessageBase* raw_instance() const { return _raw_instance; }

    template<typename T, typename ... Ts> bool Cast(T* arg, Ts* ... args);
    template<typename T> bool Cast(T** arg);

    ThriftFramedMessage();

    virtual ~ThriftFramedMessage();
  
    ThriftFramedMessage(const ThriftFramedMessage& from) = delete;
  
    ThriftFramedMessage& operator=(const ThriftFramedMessage& from) = delete;
  
    static const ::google::protobuf::Descriptor* descriptor();
    static const ThriftFramedMessage& default_instance();
  
    void Swap(ThriftFramedMessage* other);
  
    // implements Message ----------------------------------------------
  
    ThriftFramedMessage* New() const;
    void CopyFrom(const ::google::protobuf::Message& from);
    void MergeFrom(const ::google::protobuf::Message& from);
    void CopyFrom(const ThriftFramedMessage& from);
    void MergeFrom(const ThriftFramedMessage& from);
    void Clear();
    bool IsInitialized() const;
  
    int ByteSize() const;
    bool MergePartialFromCodedStream(
        ::google::protobuf::io::CodedInputStream* input);
    void SerializeWithCachedSizes(
        ::google::protobuf::io::CodedOutputStream* output) const;
    ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
    int GetCachedSize() const { return ByteSize(); }
    ::google::protobuf::Metadata GetMetadata() const;

private:
    void SharedCtor();
    void SharedDtor();
private:
friend void protobuf_AddDesc_baidu_2frpc_2fthrift_framed_5fmessage_2eproto_impl();
friend void protobuf_AddDesc_baidu_2frpc_2fthrift_framed_5fmessage_2eproto();
friend void protobuf_AssignDesc_baidu_2frpc_2fthrift_framed_5fmessage_2eproto();
friend void protobuf_ShutdownFile_baidu_2frpc_2fthrift_framed_5fmessage_2eproto();

    void InitAsDefaultInstance();
    static ThriftFramedMessage* default_instance_;
};

class ThriftStub {
public:
    explicit ThriftStub(ChannelBase* channel) : _channel(channel) {}

    template <typename REQUEST, typename RESPONSE>
    void CallMethod(const char* method_name,
                    Controller* cntl,
                    const REQUEST* raw_request,
                    RESPONSE* raw_response,
                    ::google::protobuf::Closure* done);

    void CallMethod(const char* method_name,
                    Controller* cntl,
                    const ThriftFramedMessage* req,
                    ThriftFramedMessage* res,
                    ::google::protobuf::Closure* done);

private:
    ChannelBase* _channel;
};

namespace details {

template <typename T>
class ThriftMessageWrapper final : public ThriftMessageBase {
public:
    ThriftMessageWrapper() : msg_ptr(NULL) {}
    ThriftMessageWrapper(T* msg2) : msg_ptr(msg2) {}
    virtual ~ThriftMessageWrapper() {}
    // NOTE: "T::" makes the function call work around vtable
    uint32_t Read(::apache::thrift::protocol::TProtocol* iprot) override final
    { return msg_ptr->T::read(iprot); }
    uint32_t Write(::apache::thrift::protocol::TProtocol* oprot) const override final
    { return msg_ptr->T::write(oprot); }
    T* msg_ptr;
};

template <typename T>
class ThriftMessageHolder final : public ThriftMessageBase {
public:
    virtual ~ThriftMessageHolder() {}
    // NOTE: "T::" makes the function call work around vtable
    uint32_t Read(::apache::thrift::protocol::TProtocol* iprot) override final
    { return msg.T::read(iprot); }
    uint32_t Write(::apache::thrift::protocol::TProtocol* oprot) const override final
    { return msg.T::write(oprot); }
    T msg;
};

// A wrapper closure to own additional stuffs required by ThriftStub
template <typename RESPONSE>
class ThriftDoneWrapper : public ::google::protobuf::Closure {
public:
    explicit ThriftDoneWrapper(::google::protobuf::Closure* done)
        : _done(done) {}
    void Run() override {
        _done->Run();
        delete this;
    }
private:
    ::google::protobuf::Closure* _done;
public:
    ThriftMessageWrapper<RESPONSE> raw_response_wrapper;
    ThriftFramedMessage response;
};

} // namespace details

class ThriftMessageReader {
private:
    apache::thrift::protocol::TBinaryProtocolT<apache::thrift::transport::TMemoryBuffer>* _iprot;
    std::string fname;
    uint32_t xfer;
    ::apache::thrift::protocol::TType ftype;
    int16_t fid;
public:
    explicit ThriftMessageReader(
        apache::thrift::protocol::TBinaryProtocolT<apache::thrift::transport::TMemoryBuffer>* iport) : _iprot(iport) {
            xfer = 0;
            ftype = ::apache::thrift::protocol::T_STOP;
            fid = -1;
        }

    // Thrift message recursive abort function
    template <typename T>
    bool ReadThriftMessage(T* raw_msg) {
        bool success = false;

        xfer += _iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
            raw_msg->read(_iprot);
            success = true;
        } else {
            xfer += _iprot->skip(ftype);
        }
        xfer += _iprot->readFieldEnd();
        return success;
    }

    bool ReadThriftMessage(std::string* raw_msg) {
        bool success = false;

        xfer += _iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_STRING) {
            _iprot->readString(*raw_msg);
            success = true;
        } else {
            xfer += _iprot->skip(ftype);
        }
        xfer += _iprot->readFieldEnd();
        return success;
    }

    bool ReadThriftMessage(int32_t* raw_msg) {
        bool success = false;

        xfer += _iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_I32) {
            _iprot->readI32(*raw_msg);
            success = true;
        } else {
            xfer += _iprot->skip(ftype);
        }
        xfer += _iprot->readFieldEnd();
        return success;
    }

    template<typename T, typename ... Ts>
    bool ReadThriftMessage(T* arg, Ts* ... args) {
        if (!ReadThriftMessage(arg)) {
            LOG(ERROR) << "Fail to parse " << butil::class_name<T>();
        }
        return ReadThriftMessage(args ...);
    }

};

template<typename ... Ts>
bool ParseThriftStruct(const butil::IOBuf& body, Ts* ... args) {
    const size_t body_len  = body.size();
    uint8_t* thrift_buffer = (uint8_t*)malloc(body_len);
    body.copy_to(thrift_buffer, body_len);
    auto in_buffer =
        THRIFT_STDCXX::make_shared<apache::thrift::transport::TMemoryBuffer>(
            thrift_buffer, body_len,
            ::apache::thrift::transport::TMemoryBuffer::TAKE_OWNERSHIP);
    apache::thrift::protocol::TBinaryProtocolT<apache::thrift::transport::TMemoryBuffer> iprot(in_buffer);

    bool success = false;
    uint32_t xfer = 0;
    std::string fname;

    xfer += iprot.readStructBegin(fname);
    ThriftMessageReader reader(&iprot);

    success = reader.ReadThriftMessage(args ...);

    xfer += iprot.readStructEnd();
    iprot.getTransport()->readEnd();
    return success;
}

// Cast method for request
template<typename T, typename ... Ts>
bool ThriftFramedMessage::Cast(T* arg, Ts* ... args) {
    bool success = false;

    if (!body.empty()) {
        success = ParseThriftStruct(body, arg, args ...);
    }
    return success;
}

// Cast method for response, own the raw instance inside ThriftFramedMessage, and the instance will be released later
// User desn't need to take care of the response
template<typename T>
bool ThriftFramedMessage::Cast(T** arg) {
    bool success = false;

    if (body.empty()) { // handle response scenairo
        auto raw_msg_wrapper = new details::ThriftMessageHolder<T>;
        *arg = &raw_msg_wrapper->msg;
        _raw_instance = raw_msg_wrapper;
        _own_raw_instance = true;
        success = true;
    }
    return success;
}

template <typename REQUEST, typename RESPONSE>
void ThriftStub::CallMethod(const char* method_name,
                            Controller* cntl,
                            const REQUEST* raw_request,
                            RESPONSE* raw_response,
                            ::google::protobuf::Closure* done) {
    cntl->_thrift_method_name.assign(method_name);

    details::ThriftMessageWrapper<REQUEST>
        raw_request_wrapper(const_cast<REQUEST*>(raw_request));
    ThriftFramedMessage request;
    request._raw_instance = &raw_request_wrapper;

    if (done == NULL) {
        // response is guaranteed to be unused after a synchronous RPC, no
        // need to allocate it on heap.
        ThriftFramedMessage response;
        details::ThriftMessageWrapper<RESPONSE> raw_response_wrapper(raw_response);
        response._raw_instance = &raw_response_wrapper;
        _channel->CallMethod(NULL, cntl, &request, &response, NULL);
    } else {
        // Let the new_done own the response and release it after Run().
        details::ThriftDoneWrapper<RESPONSE>* new_done =
            new details::ThriftDoneWrapper<RESPONSE>(done);
        new_done->raw_response_wrapper.msg_ptr = raw_response;
        new_done->response._raw_instance = &new_done->raw_response_wrapper;
        _channel->CallMethod(NULL, cntl, &request, &new_done->response, new_done);
    }
}

} // namespace brpc

#endif // BRPC_THRIFT_MESSAGE_H
