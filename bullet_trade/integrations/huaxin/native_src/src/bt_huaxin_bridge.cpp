/*
 * 作者: BruceLee
 * 文件职责: 实现不连接厂商 SDK 的 fake/offline 华鑫 C ABI bridge。
 * 主要输入: 版本化创建参数、opaque handle、调用方提供的固定容量缓冲区。
 * 主要输出: 版本、health、生命周期事件和有界批量 drain。
 * 上下游关系: Python ctypes wrapper 调用；后续真实 bridge 必须保持同一 ABI 边界。
 * 关键约定: 不创建线程、网络连接或交易入口，所有 C++ 异常均在 C 边界内截获。
 */

#include "bt_huaxin_bridge.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>
#include <new>

struct bt_huaxin_handle {
    std::mutex mutex;
    std::deque<bt_huaxin_event> events;
    uint32_t queue_capacity;
    uint64_t dropped_events;
    uint64_t next_sequence;
};

namespace {

int64_t monotonic_nanoseconds() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}

void push_lifecycle_event(
    bt_huaxin_handle *handle,
    uint32_t event_type,
    const char *payload
) noexcept {
    if (handle == nullptr || payload == nullptr) {
        return;
    }
    if (handle->events.size() >= handle->queue_capacity) {
        ++handle->dropped_events;
        return;
    }

    bt_huaxin_event event{};
    event.abi_version = BT_HUAXIN_ABI_VERSION;
    event.struct_size = static_cast<uint32_t>(sizeof(bt_huaxin_event));
    event.event_type = event_type;
    event.sequence = handle->next_sequence++;
    event.received_ns = monotonic_nanoseconds();
    const size_t payload_size = std::min(
        std::strlen(payload),
        static_cast<size_t>(BT_HUAXIN_EVENT_PAYLOAD_CAPACITY)
    );
    event.payload_size = static_cast<uint32_t>(payload_size);
    std::memcpy(event.payload, payload, payload_size);
    handle->events.push_back(event);
}

bool valid_create_options(const bt_huaxin_create_options *options) noexcept {
    return options != nullptr &&
           options->abi_version == BT_HUAXIN_ABI_VERSION &&
           options->struct_size >= sizeof(bt_huaxin_create_options);
}

}  // namespace

extern "C" {

uint32_t bt_huaxin_abi_version(void) {
    return BT_HUAXIN_ABI_VERSION;
}

const char *bt_huaxin_bridge_version(void) {
    return "bullet-trade-huaxin-offline-fake/1";
}

const char *bt_huaxin_error_message(int32_t result) {
    switch (result) {
        case BT_HUAXIN_OK:
            return "ok";
        case BT_HUAXIN_INVALID_ARGUMENT:
            return "invalid argument";
        case BT_HUAXIN_ABI_INCOMPATIBLE:
            return "ABI incompatible";
        case BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE:
            return "struct size incompatible";
        case BT_HUAXIN_ALLOCATION_FAILED:
            return "allocation failed";
        case BT_HUAXIN_INTERNAL_ERROR:
            return "internal error";
        default:
            return "unknown result";
    }
}

int32_t bt_huaxin_create(
    const bt_huaxin_create_options *options,
    bt_huaxin_handle **out_handle
) {
    try {
        if (out_handle == nullptr || options == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        *out_handle = nullptr;
        if (options->abi_version != BT_HUAXIN_ABI_VERSION) {
            return BT_HUAXIN_ABI_INCOMPATIBLE;
        }
        if (!valid_create_options(options)) {
            return BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE;
        }
        if (options->queue_capacity < 2u || options->queue_capacity > 1000000u) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }

        bt_huaxin_handle *handle = new (std::nothrow) bt_huaxin_handle{};
        if (handle == nullptr) {
            return BT_HUAXIN_ALLOCATION_FAILED;
        }
        handle->queue_capacity = options->queue_capacity;
        handle->dropped_events = 0u;
        handle->next_sequence = 1u;
        push_lifecycle_event(handle, 1u, "{\"event\":\"bridge_created\"}");
        push_lifecycle_event(handle, 2u, "{\"event\":\"offline_ready\"}");
        *out_handle = handle;
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_destroy(bt_huaxin_handle *handle) {
    try {
        if (handle == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        delete handle;
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_get_health(
    bt_huaxin_handle *handle,
    bt_huaxin_health *out_health
) {
    try {
        if (handle == nullptr || out_health == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        if (out_health->abi_version != BT_HUAXIN_ABI_VERSION) {
            return BT_HUAXIN_ABI_INCOMPATIBLE;
        }
        if (out_health->struct_size < sizeof(bt_huaxin_health)) {
            return BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE;
        }

        std::lock_guard<std::mutex> lock(handle->mutex);
        out_health->state = BT_HUAXIN_STATE_OFFLINE_READY;
        out_health->queue_capacity = handle->queue_capacity;
        out_health->queue_size = static_cast<uint32_t>(handle->events.size());
        out_health->reserved = 0u;
        out_health->dropped_events = handle->dropped_events;
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_drain(
    bt_huaxin_handle *handle,
    bt_huaxin_event *out_events,
    uint32_t max_events,
    uint32_t *out_count
) {
    try {
        if (handle == nullptr || out_events == nullptr || out_count == nullptr || max_events == 0u) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        *out_count = 0u;
        std::lock_guard<std::mutex> lock(handle->mutex);
        const uint32_t count = std::min(
            max_events,
            static_cast<uint32_t>(handle->events.size())
        );
        for (uint32_t index = 0u; index < count; ++index) {
            out_events[index] = handle->events.front();
            handle->events.pop_front();
        }
        *out_count = count;
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

}  // extern "C"
