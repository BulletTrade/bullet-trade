#ifndef BULLET_TRADE_HUAXIN_BRIDGE_H
#define BULLET_TRADE_HUAXIN_BRIDGE_H

/*
 * 作者: BruceLee
 * 文件职责: 定义不暴露厂商类型的华鑫 flat C ABI v2 离线合同。
 * 主要输入: 版本化 POD 创建/请求结构、opaque handle 和调用方初始化的输出描述符。
 * 主要输出: 稳定错误码、schema 身份、runtime health 与 bridge-owned 批量事件缓冲区。
 * 上下游关系: Python ctypes/cffi ABI wrapper 调用；fake bridge 与未来真实 bridge 实现本合同。
 * 关键约定: 不包含 TORA 头文件、C++/STL 类型或厂商指针；所有结构严格校验版本和大小。
 */

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#if defined(BT_HUAXIN_BUILDING_BRIDGE)
#define BT_HUAXIN_API __declspec(dllexport)
#else
#define BT_HUAXIN_API __declspec(dllimport)
#endif
#else
#define BT_HUAXIN_API __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ABI major 变更表示旧结构布局必须 fail closed，不允许按前缀猜测兼容。 */
#define BT_HUAXIN_ABI_VERSION 2u
#define BT_HUAXIN_VENDOR_SCHEMA_ID_CAPACITY 64u
#define BT_HUAXIN_FIELD_SET_VERSION_CAPACITY 32u
#define BT_HUAXIN_EVENT_PAYLOAD_CAPACITY 192u
#define BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY 192u

/* 数值一经发布不得复用或改变语义。 */
typedef enum bt_huaxin_result {
    BT_HUAXIN_OK = 0,
    BT_HUAXIN_INVALID_ARGUMENT = -1,
    BT_HUAXIN_ABI_INCOMPATIBLE = -2,
    BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE = -3,
    BT_HUAXIN_ALLOCATION_FAILED = -4,
    BT_HUAXIN_INTERNAL_ERROR = -5,
    BT_HUAXIN_SCHEMA_INCOMPATIBLE = -6,
    BT_HUAXIN_BUFFER_OWNERSHIP_ERROR = -7,
    BT_HUAXIN_UNSUPPORTED_REQUEST = -8,
    BT_HUAXIN_QUEUE_FULL = -9
} bt_huaxin_result;

typedef enum bt_huaxin_state {
    BT_HUAXIN_STATE_OFFLINE_READY = 1
} bt_huaxin_state;

typedef enum bt_huaxin_request_type {
    /* 只读 fake 请求，用于验证 POD、二进制 payload 和 request_id 合同。 */
    BT_HUAXIN_REQUEST_PING = 1
} bt_huaxin_request_type;

typedef enum bt_huaxin_event_type {
    BT_HUAXIN_EVENT_BRIDGE_CREATED = 1,
    BT_HUAXIN_EVENT_OFFLINE_READY = 2,
    BT_HUAXIN_EVENT_REQUEST_COMPLETED = 3
} bt_huaxin_event_type;

/* 唯一跨边界的运行时身份；内部布局对调用方不可见。 */
typedef struct bt_huaxin_handle bt_huaxin_handle;

/*
 * schema 字段均为“显式长度 + 原始 bytes”，不要求 NUL 结尾。
 * 本结构只嵌入其他跨边界结构，不单独作为函数参数传递。
 */
typedef struct bt_huaxin_schema_identity {
    uint32_t vendor_schema_id_size;
    uint32_t field_set_version_size;
    uint8_t vendor_schema_id[BT_HUAXIN_VENDOR_SCHEMA_ID_CAPACITY];
    uint8_t field_set_version[BT_HUAXIN_FIELD_SET_VERSION_CAPACITY];
} bt_huaxin_schema_identity;

/* caller-owned；bridge 只在 create 调用期间读取，不保留指针。 */
typedef struct bt_huaxin_create_options {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t queue_capacity;
    uint32_t reserved;
    bt_huaxin_schema_identity schema;
} bt_huaxin_create_options;

/* caller-owned；调用方先填写 abi_version/struct_size/schema，bridge 再写其余字段。 */
typedef struct bt_huaxin_health {
    uint32_t abi_version;
    uint32_t struct_size;
    int32_t state;
    uint32_t queue_capacity;
    uint32_t queue_size;
    uint32_t reserved;
    uint64_t dropped_events;
    bt_huaxin_schema_identity schema;
} bt_huaxin_health;

/* caller-owned POD；bridge 只在 submit 调用期间读取，不保留 request/payload 指针。 */
typedef struct bt_huaxin_request {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t request_type;
    uint32_t payload_size;
    uint64_t request_id;
    bt_huaxin_schema_identity schema;
    uint8_t payload[BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY];
} bt_huaxin_request;

/* 单个事件是无外部指针的 POD；动态内容以显式长度保存在固定 bytes 中。 */
typedef struct bt_huaxin_event {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t event_type;
    uint32_t payload_size;
    uint64_t sequence;
    int64_t received_ns;
    uint64_t request_id;
    bt_huaxin_schema_identity schema;
    uint8_t payload[BT_HUAXIN_EVENT_PAYLOAD_CAPACITY];
} bt_huaxin_event;

/*
 * 调用方初始化 abi_version/struct_size/schema 后传给 drain。
 * 成功后 events 指向 bridge-owned 连续 bt_huaxin_event 数组，仅可读，event_stride 固定为
 * sizeof(bt_huaxin_event)。ownership_token 是进程内单调 allocation ID，不是地址；调用方
 * 必须把 allocation ID 恰好释放一次。free 依据 ID 从内部 registry 取回真实 buffer，
 * 不会信任可能损坏的 events/count/stride；调用方不得自行释放 events。空批次无分配，
 * 仍可安全 free。
 */
typedef struct bt_huaxin_event_batch {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t event_count;
    uint32_t event_stride;
    bt_huaxin_schema_identity schema;
    const bt_huaxin_event *events;
    uint64_t ownership_token;
} bt_huaxin_event_batch;

/* 返回 bridge 静态存储；调用方不得 free 或改写。 */
BT_HUAXIN_API uint32_t bt_huaxin_abi_version(void);
BT_HUAXIN_API const char *bt_huaxin_bridge_version(void);
BT_HUAXIN_API const char *bt_huaxin_vendor_schema_id(void);
BT_HUAXIN_API const char *bt_huaxin_field_set_version(void);
BT_HUAXIN_API const char *bt_huaxin_error_message(int32_t result);

/* 成功返回 opaque handle；调用方拥有 handle，必须恰好 destroy 一次。 */
BT_HUAXIN_API int32_t bt_huaxin_create(
    const bt_huaxin_create_options *options,
    bt_huaxin_handle **out_handle
);

BT_HUAXIN_API int32_t bt_huaxin_destroy(bt_huaxin_handle *handle);

BT_HUAXIN_API int32_t bt_huaxin_get_health(
    bt_huaxin_handle *handle,
    bt_huaxin_health *out_health
);

BT_HUAXIN_API int32_t bt_huaxin_submit_request(
    bt_huaxin_handle *handle,
    const bt_huaxin_request *request
);

/* 成功时把最多 max_events 个事件所有权放入 out_batch；max_events 必须大于零。 */
BT_HUAXIN_API int32_t bt_huaxin_drain_event_batch(
    bt_huaxin_handle *handle,
    uint32_t max_events,
    bt_huaxin_event_batch *out_batch
);

/*
 * 释放 bridge-owned event buffer，并把描述符置为空；同一非空描述符不得释放两次。
 * header 不兼容时不读取 allocation ID；header 合法且 ID 仍在 registry 时，即使 schema
 * 或 events/count/stride 被损坏也会先安全回收真实 buffer，再返回对应的稳定负码。
 */
BT_HUAXIN_API int32_t bt_huaxin_free_event_batch(bt_huaxin_event_batch *batch);

#ifdef __cplusplus
}
#endif

#endif
