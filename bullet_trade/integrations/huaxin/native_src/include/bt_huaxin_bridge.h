#ifndef BULLET_TRADE_HUAXIN_BRIDGE_H
#define BULLET_TRADE_HUAXIN_BRIDGE_H

/*
 * 作者: BruceLee
 * 文件职责: 定义不暴露厂商类型的华鑫 flat C ABI 离线合同。
 * 主要输入: 版本化创建参数、opaque handle、health/event 输出缓冲区。
 * 主要输出: 稳定错误码、bridge 版本、runtime health 与有界批量事件。
 * 上下游关系: Python ctypes wrapper 调用；fake bridge 实现本合同。
 * 关键约定: 本头文件不包含任何 TORA 头文件、STL 类型、凭据或网络接口。
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

#define BT_HUAXIN_ABI_VERSION 1u
#define BT_HUAXIN_EVENT_PAYLOAD_CAPACITY 192u

typedef enum bt_huaxin_result {
    BT_HUAXIN_OK = 0,
    BT_HUAXIN_INVALID_ARGUMENT = -1,
    BT_HUAXIN_ABI_INCOMPATIBLE = -2,
    BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE = -3,
    BT_HUAXIN_ALLOCATION_FAILED = -4,
    BT_HUAXIN_INTERNAL_ERROR = -5
} bt_huaxin_result;

typedef enum bt_huaxin_state {
    BT_HUAXIN_STATE_OFFLINE_READY = 1
} bt_huaxin_state;

typedef struct bt_huaxin_handle bt_huaxin_handle;

typedef struct bt_huaxin_create_options {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t queue_capacity;
    uint32_t reserved;
} bt_huaxin_create_options;

typedef struct bt_huaxin_health {
    uint32_t abi_version;
    uint32_t struct_size;
    int32_t state;
    uint32_t queue_capacity;
    uint32_t queue_size;
    uint32_t reserved;
    uint64_t dropped_events;
} bt_huaxin_health;

typedef struct bt_huaxin_event {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t event_type;
    uint32_t payload_size;
    uint64_t sequence;
    int64_t received_ns;
    uint8_t payload[BT_HUAXIN_EVENT_PAYLOAD_CAPACITY];
} bt_huaxin_event;

BT_HUAXIN_API uint32_t bt_huaxin_abi_version(void);
BT_HUAXIN_API const char *bt_huaxin_bridge_version(void);
BT_HUAXIN_API const char *bt_huaxin_error_message(int32_t result);

BT_HUAXIN_API int32_t bt_huaxin_create(
    const bt_huaxin_create_options *options,
    bt_huaxin_handle **out_handle
);

BT_HUAXIN_API int32_t bt_huaxin_destroy(bt_huaxin_handle *handle);

BT_HUAXIN_API int32_t bt_huaxin_get_health(
    bt_huaxin_handle *handle,
    bt_huaxin_health *out_health
);

BT_HUAXIN_API int32_t bt_huaxin_drain(
    bt_huaxin_handle *handle,
    bt_huaxin_event *out_events,
    uint32_t max_events,
    uint32_t *out_count
);

#ifdef __cplusplus
}
#endif

#endif
