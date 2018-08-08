
namespace cpp example

struct EchoRequest {
    1: optional string data;
    2: optional i32 need_by_proxy;
}

struct ProxyRequest {
    2: optional i32 need_by_proxy;
}

struct EchoResponse {
    1: required string data;
}

service EchoService {
    //EchoResponse Echo(1:EchoRequest request);
    EchoResponse Echo(1:EchoRequest request, 2: required i32 num, 3: string name);
}

