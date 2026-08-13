/*
 * 作者: BruceLee
 * 文件职责: 实现不连接厂商 SDK 的 fake/offline 华鑫 C ABI v2。
 * 主要输入: 精确版本/尺寸/schema 的 POD 参数、opaque handle 和调用方输出描述符。
 * 主要输出: health、只读请求回执及需显式 free 的 bridge-owned 批量事件缓冲区。
 * 上下游关系: Python ctypes wrapper 调用；未来真实 bridge 必须保持同一 flat ABI 边界。
 * 关键约定: 不创建网络或交易入口；C++/STL/异常与任何内部指针均不越过公开签名。
 */

#include "bt_huaxin_bridge.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <new>
#include <type_traits>
#include <unordered_map>

struct bt_huaxin_handle {
    std::mutex mutex;
    std::deque<bt_huaxin_event> events;
    uint32_t queue_capacity;
    uint64_t dropped_events;
    uint64_t next_sequence;
};

namespace {

constexpr char kVendorSchemaId[] = "bullet_trade.huaxin.offline_fake.v1";
constexpr char kFieldSetVersion[] = "1";
constexpr uint32_t kVendorSchemaIdSize =
    static_cast<uint32_t>(sizeof(kVendorSchemaId) - 1u);
constexpr uint32_t kFieldSetVersionSize =
    static_cast<uint32_t>(sizeof(kFieldSetVersion) - 1u);

std::mutex g_batch_allocation_mutex;
struct BatchAllocation {
    void *events;
    uint32_t event_count;
};
std::unordered_map<uint64_t, BatchAllocation> g_batch_allocations;
uint64_t g_next_batch_allocation_id = 1u;

static_assert(kVendorSchemaIdSize <= BT_HUAXIN_VENDOR_SCHEMA_ID_CAPACITY);
static_assert(kFieldSetVersionSize <= BT_HUAXIN_FIELD_SET_VERSION_CAPACITY);
static_assert(std::is_standard_layout<bt_huaxin_create_options>::value);
static_assert(std::is_trivially_copyable<bt_huaxin_create_options>::value);
static_assert(std::is_standard_layout<bt_huaxin_health>::value);
static_assert(std::is_trivially_copyable<bt_huaxin_health>::value);
static_assert(std::is_standard_layout<bt_huaxin_request>::value);
static_assert(std::is_trivially_copyable<bt_huaxin_request>::value);
static_assert(std::is_standard_layout<bt_huaxin_event>::value);
static_assert(std::is_trivially_copyable<bt_huaxin_event>::value);
static_assert(std::is_standard_layout<bt_huaxin_event_batch>::value);
static_assert(std::is_trivially_copyable<bt_huaxin_event_batch>::value);

int64_t monotonic_nanoseconds() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}

bool register_batch_allocation(
    void *events,
    uint32_t event_count,
    uint64_t *out_token
) noexcept {
    try {
        if (events == nullptr || event_count == 0u || out_token == nullptr) {
            return false;
        }
        std::lock_guard<std::mutex> lock(g_batch_allocation_mutex);
        if (g_next_batch_allocation_id == 0u) {
            return false;
        }
        const uint64_t token = g_next_batch_allocation_id++;
        const bool inserted = g_batch_allocations.emplace(
            token,
            BatchAllocation{events, event_count}
        ).second;
        if (inserted) {
            *out_token = token;
        }
        return inserted;
    } catch (...) {
        return false;
    }
}

bool claim_batch_allocation(uint64_t token, BatchAllocation *out_allocation) noexcept {
    try {
        if (token == 0u || out_allocation == nullptr) {
            return false;
        }
        std::lock_guard<std::mutex> lock(g_batch_allocation_mutex);
        const auto found = g_batch_allocations.find(token);
        if (found == g_batch_allocations.end()) {
            return false;
        }
        *out_allocation = found->second;
        g_batch_allocations.erase(found);
        return true;
    } catch (...) {
        return false;
    }
}

void set_schema_identity(bt_huaxin_schema_identity *identity) noexcept {
    if (identity == nullptr) {
        return;
    }
    std::memset(identity, 0, sizeof(*identity));
    identity->vendor_schema_id_size = kVendorSchemaIdSize;
    identity->field_set_version_size = kFieldSetVersionSize;
    std::memcpy(identity->vendor_schema_id, kVendorSchemaId, kVendorSchemaIdSize);
    std::memcpy(identity->field_set_version, kFieldSetVersion, kFieldSetVersionSize);
}

bool valid_schema_lengths(const bt_huaxin_schema_identity &identity) noexcept {
    return identity.vendor_schema_id_size <= BT_HUAXIN_VENDOR_SCHEMA_ID_CAPACITY &&
           identity.field_set_version_size <= BT_HUAXIN_FIELD_SET_VERSION_CAPACITY;
}

bool schema_identity_matches(const bt_huaxin_schema_identity &identity) noexcept {
    if (!valid_schema_lengths(identity) ||
        identity.vendor_schema_id_size != kVendorSchemaIdSize ||
        identity.field_set_version_size != kFieldSetVersionSize) {
        return false;
    }
    return std::memcmp(identity.vendor_schema_id, kVendorSchemaId, kVendorSchemaIdSize) == 0 &&
           std::memcmp(
               identity.field_set_version,
               kFieldSetVersion,
               kFieldSetVersionSize
           ) == 0;
}

int32_t validate_header(uint32_t abi_version, uint32_t struct_size, size_t expected_size) noexcept {
    if (abi_version != BT_HUAXIN_ABI_VERSION) {
        return BT_HUAXIN_ABI_INCOMPATIBLE;
    }
    if (struct_size != expected_size) {
        return BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE;
    }
    return BT_HUAXIN_OK;
}

int32_t validate_create_options(const bt_huaxin_create_options *options) noexcept {
    if (options == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        options->abi_version,
        options->struct_size,
        sizeof(bt_huaxin_create_options)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    if (!schema_identity_matches(options->schema)) {
        return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
    }
    if (options->reserved != 0u ||
        options->queue_capacity < 2u ||
        options->queue_capacity > 1000000u) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    return BT_HUAXIN_OK;
}

int32_t validate_health_descriptor(const bt_huaxin_health *health) noexcept {
    if (health == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        health->abi_version,
        health->struct_size,
        sizeof(bt_huaxin_health)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    return schema_identity_matches(health->schema)
        ? BT_HUAXIN_OK
        : BT_HUAXIN_SCHEMA_INCOMPATIBLE;
}

int32_t validate_request(const bt_huaxin_request *request) noexcept {
    if (request == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        request->abi_version,
        request->struct_size,
        sizeof(bt_huaxin_request)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    if (!schema_identity_matches(request->schema)) {
        return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
    }
    if (request->payload_size > BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY ||
        request->request_id == 0u) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    if (request->request_type != BT_HUAXIN_REQUEST_PING) {
        return BT_HUAXIN_UNSUPPORTED_REQUEST;
    }
    return BT_HUAXIN_OK;
}

int32_t validate_batch_descriptor(const bt_huaxin_event_batch *batch) noexcept {
    if (batch == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        batch->abi_version,
        batch->struct_size,
        sizeof(bt_huaxin_event_batch)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    return schema_identity_matches(batch->schema)
        ? BT_HUAXIN_OK
        : BT_HUAXIN_SCHEMA_INCOMPATIBLE;
}

int32_t push_event_locked(
    bt_huaxin_handle *handle,
    uint32_t event_type,
    uint64_t request_id,
    const uint8_t *payload,
    uint32_t payload_size
) noexcept {
    if (handle == nullptr || payload_size > BT_HUAXIN_EVENT_PAYLOAD_CAPACITY ||
        (payload == nullptr && payload_size != 0u)) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    if (handle->events.size() >= handle->queue_capacity) {
        ++handle->dropped_events;
        return BT_HUAXIN_QUEUE_FULL;
    }
    try {
        bt_huaxin_event event{};
        event.abi_version = BT_HUAXIN_ABI_VERSION;
        event.struct_size = static_cast<uint32_t>(sizeof(bt_huaxin_event));
        event.event_type = event_type;
        event.payload_size = payload_size;
        event.sequence = handle->next_sequence;
        event.received_ns = monotonic_nanoseconds();
        event.request_id = request_id;
        set_schema_identity(&event.schema);
        if (payload_size != 0u) {
            std::memcpy(event.payload, payload, payload_size);
        }
        handle->events.push_back(event);
        ++handle->next_sequence;
        return BT_HUAXIN_OK;
    } catch (const std::bad_alloc &) {
        return BT_HUAXIN_ALLOCATION_FAILED;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t push_literal_event_locked(
    bt_huaxin_handle *handle,
    uint32_t event_type,
    const char *payload,
    size_t payload_size
) noexcept {
    if (payload_size > BT_HUAXIN_EVENT_PAYLOAD_CAPACITY) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    return push_event_locked(
        handle,
        event_type,
        0u,
        reinterpret_cast<const uint8_t *>(payload),
        static_cast<uint32_t>(payload_size)
    );
}

}  // namespace

extern "C" {

uint32_t bt_huaxin_abi_version(void) {
    return BT_HUAXIN_ABI_VERSION;
}

const char *bt_huaxin_bridge_version(void) {
    return "bullet-trade-huaxin-offline-fake/2";
}

const char *bt_huaxin_vendor_schema_id(void) {
    return kVendorSchemaId;
}

const char *bt_huaxin_field_set_version(void) {
    return kFieldSetVersion;
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
        case BT_HUAXIN_SCHEMA_INCOMPATIBLE:
            return "vendor schema or field set incompatible";
        case BT_HUAXIN_BUFFER_OWNERSHIP_ERROR:
            return "buffer ownership error";
        case BT_HUAXIN_UNSUPPORTED_REQUEST:
            return "unsupported request";
        case BT_HUAXIN_QUEUE_FULL:
            return "queue full";
        default:
            return "unknown result";
    }
}

int32_t bt_huaxin_create(
    const bt_huaxin_create_options *options,
    bt_huaxin_handle **out_handle
) {
    try {
        if (out_handle == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        *out_handle = nullptr;
        const int32_t validation_result = validate_create_options(options);
        if (validation_result != BT_HUAXIN_OK) {
            return validation_result;
        }

        std::unique_ptr<bt_huaxin_handle> handle{
            new (std::nothrow) bt_huaxin_handle{}
        };
        if (handle == nullptr) {
            return BT_HUAXIN_ALLOCATION_FAILED;
        }
        handle->queue_capacity = options->queue_capacity;
        handle->dropped_events = 0u;
        handle->next_sequence = 1u;
        constexpr char kCreated[] = "{\"event\":\"bridge_created\"}";
        constexpr char kReady[] = "{\"event\":\"offline_ready\"}";
        const int32_t created_result = push_literal_event_locked(
            handle.get(),
            BT_HUAXIN_EVENT_BRIDGE_CREATED,
            kCreated,
            sizeof(kCreated) - 1u
        );
        if (created_result != BT_HUAXIN_OK) {
            return created_result;
        }
        const int32_t ready_result = push_literal_event_locked(
            handle.get(),
            BT_HUAXIN_EVENT_OFFLINE_READY,
            kReady,
            sizeof(kReady) - 1u
        );
        if (ready_result != BT_HUAXIN_OK) {
            return ready_result;
        }
        *out_handle = handle.release();
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
        if (handle == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        const int32_t validation_result = validate_health_descriptor(out_health);
        if (validation_result != BT_HUAXIN_OK) {
            return validation_result;
        }

        std::lock_guard<std::mutex> lock(handle->mutex);
        out_health->state = BT_HUAXIN_STATE_OFFLINE_READY;
        out_health->queue_capacity = handle->queue_capacity;
        out_health->queue_size = static_cast<uint32_t>(handle->events.size());
        out_health->reserved = 0u;
        out_health->dropped_events = handle->dropped_events;
        set_schema_identity(&out_health->schema);
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_submit_request(
    bt_huaxin_handle *handle,
    const bt_huaxin_request *request
) {
    try {
        if (handle == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        const int32_t validation_result = validate_request(request);
        if (validation_result != BT_HUAXIN_OK) {
            return validation_result;
        }

        std::lock_guard<std::mutex> lock(handle->mutex);
        constexpr char kDefaultReply[] = "{\"event\":\"request_completed\"}";
        const uint8_t *payload = request->payload_size == 0u
            ? reinterpret_cast<const uint8_t *>(kDefaultReply)
            : request->payload;
        const uint32_t payload_size = request->payload_size == 0u
            ? static_cast<uint32_t>(sizeof(kDefaultReply) - 1u)
            : request->payload_size;
        return push_event_locked(
            handle,
            BT_HUAXIN_EVENT_REQUEST_COMPLETED,
            request->request_id,
            payload,
            payload_size
        );
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_drain_event_batch(
    bt_huaxin_handle *handle,
    uint32_t max_events,
    bt_huaxin_event_batch *out_batch
) {
    try {
        if (handle == nullptr || max_events == 0u) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        const int32_t validation_result = validate_batch_descriptor(out_batch);
        if (validation_result != BT_HUAXIN_OK) {
            return validation_result;
        }
        if (out_batch->events != nullptr || out_batch->ownership_token != 0u ||
            out_batch->event_count != 0u || out_batch->event_stride != 0u) {
            return BT_HUAXIN_BUFFER_OWNERSHIP_ERROR;
        }

        std::lock_guard<std::mutex> lock(handle->mutex);
        const uint32_t count = std::min(
            max_events,
            static_cast<uint32_t>(handle->events.size())
        );
        out_batch->event_stride = static_cast<uint32_t>(sizeof(bt_huaxin_event));
        set_schema_identity(&out_batch->schema);
        if (count == 0u) {
            return BT_HUAXIN_OK;
        }

        auto *events = static_cast<bt_huaxin_event *>(
            std::calloc(static_cast<size_t>(count), sizeof(bt_huaxin_event))
        );
        if (events == nullptr) {
            out_batch->event_stride = 0u;
            return BT_HUAXIN_ALLOCATION_FAILED;
        }
        uint64_t ownership_token = 0u;
        if (!register_batch_allocation(events, count, &ownership_token)) {
            std::free(events);
            out_batch->event_stride = 0u;
            return BT_HUAXIN_ALLOCATION_FAILED;
        }
        for (uint32_t index = 0u; index < count; ++index) {
            events[index] = handle->events.front();
            handle->events.pop_front();
        }
        out_batch->event_count = count;
        out_batch->events = events;
        out_batch->ownership_token = ownership_token;
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_free_event_batch(bt_huaxin_event_batch *batch) {
    try {
        if (batch == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        const int32_t validation_result = validate_header(
            batch->abi_version,
            batch->struct_size,
            sizeof(bt_huaxin_event_batch)
        );
        if (validation_result != BT_HUAXIN_OK) {
            return validation_result;
        }
        const int32_t schema_result = schema_identity_matches(batch->schema)
            ? BT_HUAXIN_OK
            : BT_HUAXIN_SCHEMA_INCOMPATIBLE;
        if (batch->ownership_token == 0u) {
            const bool metadata_matches =
                batch->events == nullptr && batch->event_count == 0u &&
                (batch->event_stride == 0u ||
                 batch->event_stride == sizeof(bt_huaxin_event));
            batch->event_count = 0u;
            batch->event_stride = 0u;
            set_schema_identity(&batch->schema);
            if (schema_result != BT_HUAXIN_OK) {
                return schema_result;
            }
            return metadata_matches ? BT_HUAXIN_OK : BT_HUAXIN_BUFFER_OWNERSHIP_ERROR;
        }
        BatchAllocation allocation{};
        if (!claim_batch_allocation(batch->ownership_token, &allocation)) {
            return BT_HUAXIN_BUFFER_OWNERSHIP_ERROR;
        }

        const bool metadata_matches =
            batch->events == allocation.events &&
            batch->event_count == allocation.event_count &&
            batch->event_stride == sizeof(bt_huaxin_event);
        std::free(allocation.events);
        batch->events = nullptr;
        batch->ownership_token = 0u;
        batch->event_count = 0u;
        batch->event_stride = 0u;
        set_schema_identity(&batch->schema);
        if (schema_result != BT_HUAXIN_OK) {
            return schema_result;
        }
        return metadata_matches ? BT_HUAXIN_OK : BT_HUAXIN_BUFFER_OWNERSHIP_ERROR;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

}  // extern "C"
