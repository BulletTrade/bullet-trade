/*
 * 作者: BruceLee
 * 文件职责: 实现 Linux x86_64 华鑫 TORA Trader-only flat C ABI bridge。
 * 主要输入: 调用方深拷贝会话配置、六类查询、受控沪深委托和明确身份撤单 POD。
 * 主要输出: 登录/readiness health 与 bridge-owned 结构化 Trader 事件批次。
 * 上下游关系: Python native.py 调用；下游仅链接官方 libtraderapi.so/Trader 头文件。
 * 关键环境或配置: 交易和撤单独立默认关闭；回调立即深拷贝，不向 ABI 暴露厂商指针。
 */

#include "bt_huaxin_bridge.h"

#include "TORATstpTraderApi.h"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <type_traits>
#include <unordered_map>

using namespace TORASTOCKAPI;

struct bt_huaxin_handle;

namespace {

constexpr char kVendorSchemaId[] = "bullet_trade.huaxin.tora_trader.v1";
constexpr char kFieldSetVersion[] = "tora-stock-v4.1.8-order-v2";
constexpr uint32_t kVendorSchemaIdSize =
    static_cast<uint32_t>(sizeof(kVendorSchemaId) - 1u);
constexpr uint32_t kFieldSetVersionSize =
    static_cast<uint32_t>(sizeof(kFieldSetVersion) - 1u);
constexpr uint32_t kInitialShareholder = 1u << 0u;
constexpr uint32_t kInitialAccount = 1u << 1u;
constexpr uint32_t kInitialPosition = 1u << 2u;
constexpr uint32_t kInitialOrder = 1u << 3u;
constexpr uint32_t kInitialTrade = 1u << 4u;
constexpr uint32_t kInitialQueryMask =
    kInitialShareholder | kInitialAccount | kInitialPosition |
    kInitialOrder | kInitialTrade;

void secure_clear_bytes(void *data, size_t size) noexcept {
    volatile uint8_t *bytes = static_cast<volatile uint8_t *>(data);
    for (size_t index = 0u; index < size; ++index) {
        bytes[index] = 0u;
    }
}

void secure_clear_string(std::string *value) noexcept {
    if (value == nullptr) {
        return;
    }
    if (!value->empty()) {
        secure_clear_bytes(&(*value)[0], value->size());
    }
    value->clear();
}

struct SessionValues {
    std::string flow_path;
    std::string trade_front;
    std::string login_account;
    std::string department_id;
    std::string password;
    std::string dynamic_password;
    std::string user_product_info;
    std::string interface_product_info;
    std::string terminal_info;
    std::string mac_address;
    std::string interface_address;
    int32_t login_account_type = BT_HUAXIN_LOGIN_ACCOUNT_ID;
    int32_t trade_comm_mode = BT_HUAXIN_TRADE_COMM_TCP;
    int32_t private_topic = BT_HUAXIN_TOPIC_RESUME;
    int32_t public_topic = BT_HUAXIN_TOPIC_DISABLED;
    bool encrypt = false;
    bool enable_trading = false;
    bool enable_cancel = false;

    SessionValues() = default;
    SessionValues(const SessionValues &) = default;
    SessionValues &operator=(const SessionValues &) = default;

    void clear_sensitive() noexcept {
        secure_clear_string(&flow_path);
        secure_clear_string(&trade_front);
        secure_clear_string(&login_account);
        secure_clear_string(&department_id);
        secure_clear_string(&password);
        secure_clear_string(&dynamic_password);
        secure_clear_string(&user_product_info);
        secure_clear_string(&interface_product_info);
        secure_clear_string(&terminal_info);
        secure_clear_string(&mac_address);
        secure_clear_string(&interface_address);
    }

    ~SessionValues() {
        clear_sensitive();
    }
};

struct QueryProgress {
    uint32_t request_type;
    uint32_t record_count;
};

class TraderSpi final : public CTORATstpTraderSpi {
public:
    explicit TraderSpi(bt_huaxin_handle *handle) noexcept : handle_(handle) {}

    void OnFrontConnected() override;
    void OnFrontDisconnected(int reason) override;
    void OnRspError(CTORATstpRspInfoField *info, int request_id, bool is_last) override;
    void OnRspGetConnectionInfo(
        CTORATstpConnectionInfoField *connection,
        CTORATstpRspInfoField *info,
        int request_id
    ) override;
    void OnRspUserLogin(
        CTORATstpRspUserLoginField *login,
        CTORATstpRspInfoField *info,
        int request_id
    ) override;
    void OnRspQrySecurity(
        CTORATstpSecurityField *field,
        CTORATstpRspInfoField *info,
        int request_id,
        bool is_last
    ) override;
    void OnRspQryShareholderAccount(
        CTORATstpShareholderAccountField *field,
        CTORATstpRspInfoField *info,
        int request_id,
        bool is_last
    ) override;
    void OnRspQryTradingAccount(
        CTORATstpTradingAccountField *field,
        CTORATstpRspInfoField *info,
        int request_id,
        bool is_last
    ) override;
    void OnRspQryPosition(
        CTORATstpPositionField *field,
        CTORATstpRspInfoField *info,
        int request_id,
        bool is_last
    ) override;
    void OnRspQryOrder(
        CTORATstpOrderField *field,
        CTORATstpRspInfoField *info,
        int request_id,
        bool is_last
    ) override;
    void OnRspQryTrade(
        CTORATstpTradeField *field,
        CTORATstpRspInfoField *info,
        int request_id,
        bool is_last
    ) override;
    void OnRspOrderInsert(
        CTORATstpInputOrderField *field,
        CTORATstpRspInfoField *info,
        int request_id
    ) override;
    void OnErrRtnOrderInsert(
        CTORATstpInputOrderField *field,
        CTORATstpRspInfoField *info,
        int request_id
    ) override;
    void OnRspOrderAction(
        CTORATstpInputOrderActionField *field,
        CTORATstpRspInfoField *info,
        int request_id
    ) override;
    void OnErrRtnOrderAction(
        CTORATstpInputOrderActionField *field,
        CTORATstpRspInfoField *info,
        int request_id
    ) override;
    void OnRtnOrder(CTORATstpOrderField *field) override;
    void OnRtnTrade(CTORATstpTradeField *field) override;

private:
    bt_huaxin_handle *handle_;
};

struct BatchAllocation {
    void *events;
    uint32_t event_count;
};

std::mutex g_batch_allocation_mutex;
std::unordered_map<uint64_t, BatchAllocation> g_batch_allocations;
uint64_t g_next_batch_allocation_id = 1u;

static_assert(kVendorSchemaIdSize <= BT_HUAXIN_VENDOR_SCHEMA_ID_CAPACITY);
static_assert(kFieldSetVersionSize <= BT_HUAXIN_FIELD_SET_VERSION_CAPACITY);
static_assert(sizeof(bt_huaxin_query_request) <= BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_limit_order_request) <= BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_order_request) <= BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_order_request) == sizeof(bt_huaxin_limit_order_request));
static_assert(sizeof(bt_huaxin_order_request) == 160u);
static_assert(offsetof(bt_huaxin_order_request, direction) == 20u);
static_assert(offsetof(bt_huaxin_order_request, order_price_type) == 21u);
static_assert(offsetof(bt_huaxin_order_request, time_condition) == 22u);
static_assert(offsetof(bt_huaxin_order_request, volume_condition) == 23u);
static_assert(offsetof(bt_huaxin_order_request, limit_price) == 24u);
static_assert(offsetof(bt_huaxin_order_request, exchange) == 40u);
static_assert(sizeof(bt_huaxin_cancel_order_request) <= BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_state_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_error_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_login_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_security_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_security_event) == 360u);
static_assert(offsetof(bt_huaxin_security_event, market_buy_unit) == 284u);
static_assert(offsetof(bt_huaxin_security_event, security_status) == 320u);
static_assert(offsetof(bt_huaxin_security_event, upper_limit_price) == 344u);
static_assert(offsetof(bt_huaxin_security_event, lower_limit_price) == 352u);
static_assert(sizeof(bt_huaxin_shareholder_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_account_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_position_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_order_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_order_event) == 504u);
static_assert(offsetof(bt_huaxin_order_event, direction) == 124u);
static_assert(offsetof(bt_huaxin_order_event, order_price_type) == 125u);
static_assert(offsetof(bt_huaxin_order_event, time_condition) == 126u);
static_assert(offsetof(bt_huaxin_order_event, volume_condition) == 127u);
static_assert(offsetof(bt_huaxin_order_event, limit_price) == 136u);
static_assert(sizeof(bt_huaxin_trade_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_query_end_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(sizeof(bt_huaxin_order_response_event) <= BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY);
static_assert(std::is_trivially_copyable<bt_huaxin_owned_event>::value);
static_assert(std::is_same<TTORATstpVolumeType, int>::value);
static_assert(std::is_same<TTORATstpPriceType, double>::value);
static_assert(std::is_same<TTORATstpMoneyType, double>::value);
static_assert(
    sizeof(TTORATstpUserProductInfoType) == BT_HUAXIN_USER_PRODUCT_INFO_CAPACITY + 1u
);
static_assert(
    sizeof(TTORATstpInterfaceProductInfoType) ==
    BT_HUAXIN_INTERFACE_PRODUCT_INFO_CAPACITY + 1u
);
static_assert(sizeof(TTORATstpTerminalInfoType) == BT_HUAXIN_TERMINAL_INFO_CAPACITY + 1u);
static_assert(sizeof(TTORATstpMacAddressType) == BT_HUAXIN_MAC_ADDRESS_CAPACITY + 1u);
static_assert(
    std::numeric_limits<TTORATstpVolumeType>::max() >=
    std::numeric_limits<int32_t>::max()
);

constexpr void copy_position_numeric_fields(
    const CTORATstpPositionField &field,
    bt_huaxin_position_event *event
) noexcept {
    event->current_position = field.CurrentPosition;
    event->available_position = field.AvailablePosition;
    event->history_position = field.HistoryPos;
    event->history_frozen = field.HistoryPosFrozen;
    event->today_bs = field.TodayBSPos;
    event->today_bs_frozen = field.TodayBSPosFrozen;
    event->today_pr = field.TodayPRPos;
    event->today_pr_frozen = field.TodayPRPosFrozen;
    event->total_cost = field.TotalPosCost;
    event->today_sm = field.TodaySMPos;
    event->today_sm_frozen = field.TodaySMPosFrozen;
    event->pre_position = field.PrePosition;
    event->pre_frozen = field.PreFrozen;
    event->repay_untrade_volume = field.RepayUntradeVolume;
    event->repay_transfer_untrade_volume = field.RepayTransferUntradeVolume;
    event->collateral_buy_untrade_volume = field.CollateralBuyUntradeVolume;
    event->credit_buy_untrade_volume = field.CreditBuyUntradeVolume;
    event->credit_sell_untrade_volume = field.CreditSellUntradeVolume;
    event->history_position_price = field.HistoryPosPrice;
    event->open_position_cost = field.OpenPosCost;
    event->collateral_buy_untrade_amount = field.CollateralBuyUntradeAmount;
    event->credit_buy_untrade_amount = field.CreditBuyUntradeAmount;
    event->credit_sell_untrade_amount = field.CreditSellUntradeAmount;
}

constexpr bool position_mapping_contract_fixture() noexcept {
    CTORATstpPositionField field{};
    field.CurrentPosition = 901;
    field.AvailablePosition = 407;
    field.HistoryPos = 701;
    field.HistoryPosFrozen = 11;
    field.TodayBSPos = 113;
    field.TodayBSPosFrozen = 13;
    field.TodayPRPos = 17;
    field.TodayPRPosFrozen = 19;
    field.TodaySMPos = 23;
    field.TodaySMPosFrozen = 29;
    field.PrePosition = 659;
    field.PreFrozen = 31;
    field.RepayUntradeVolume = 37;
    field.RepayTransferUntradeVolume = 41;
    field.CollateralBuyUntradeVolume = 43;
    field.CreditBuyUntradeVolume = 47;
    field.CreditSellUntradeVolume = 53;
    field.HistoryPosPrice = 61.25;
    field.TotalPosCost = 67890.5;
    field.OpenPosCost = 71.75;
    field.CollateralBuyUntradeAmount = 73.25;
    field.CreditBuyUntradeAmount = 79.5;
    field.CreditSellUntradeAmount = 83.75;
    bt_huaxin_position_event event{};
    copy_position_numeric_fields(field, &event);
    return event.current_position == 901 && event.available_position == 407 &&
           event.history_position == 701 && event.history_frozen == 11 &&
           event.today_bs == 113 && event.today_bs_frozen == 13 &&
           event.today_pr == 17 && event.today_pr_frozen == 19 &&
           event.today_sm == 23 && event.today_sm_frozen == 29 &&
           event.pre_position == 659 && event.pre_frozen == 31 &&
           event.repay_untrade_volume == 37 &&
           event.repay_transfer_untrade_volume == 41 &&
           event.collateral_buy_untrade_volume == 43 &&
           event.credit_buy_untrade_volume == 47 &&
           event.credit_sell_untrade_volume == 53 &&
           event.history_position_price == 61.25 && event.total_cost == 67890.5 &&
           event.open_position_cost == 71.75 &&
           event.collateral_buy_untrade_amount == 73.25 &&
           event.credit_buy_untrade_amount == 79.5 &&
           event.credit_sell_untrade_amount == 83.75;
}

static_assert(
    position_mapping_contract_fixture(),
    "TORA Position must copy authoritative balance, availability, frozen, and in-flight fields"
);

}  // namespace

struct bt_huaxin_handle {
    std::mutex mutex;
    std::deque<bt_huaxin_owned_event> events;
    uint32_t queue_capacity = 0u;
    uint64_t dropped_events = 0u;
    uint64_t next_sequence = 1u;
    int32_t state = BT_HUAXIN_STATE_CREATED;
    bool transport_connected = false;
    bool logged_in = false;
    bool ready_for_queries = false;
    uint64_t session_epoch = 0u;
    int32_t last_error_id = 0;
    int32_t front_id = 0;
    int32_t session_id = 0;
    int32_t max_order_ref = 0;
    uint32_t initial_query_mask = 0u;
    SessionValues session;
    CTORATstpTraderApi *api = nullptr;
    TraderSpi *spi = nullptr;
    std::unordered_map<int, QueryProgress> queries;
};

namespace {

int64_t monotonic_nanoseconds() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
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
    return valid_schema_lengths(identity) &&
           identity.vendor_schema_id_size == kVendorSchemaIdSize &&
           identity.field_set_version_size == kFieldSetVersionSize &&
           std::memcmp(identity.vendor_schema_id, kVendorSchemaId, kVendorSchemaIdSize) == 0 &&
           std::memcmp(identity.field_set_version, kFieldSetVersion, kFieldSetVersionSize) == 0;
}

int32_t validate_header(uint32_t abi_version, uint32_t struct_size, size_t expected) noexcept {
    if (abi_version != BT_HUAXIN_ABI_VERSION) {
        return BT_HUAXIN_ABI_INCOMPATIBLE;
    }
    return struct_size == expected
        ? BT_HUAXIN_OK
        : BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE;
}

size_t bounded_text_length(const char *text, size_t capacity) noexcept {
    if (text == nullptr) {
        return 0u;
    }
    size_t size = 0u;
    while (size < capacity && text[size] != '\0') {
        ++size;
    }
    return size;
}

bool bytes_are_c_text(const uint8_t *bytes, uint32_t size, uint32_t capacity) noexcept {
    if ((bytes == nullptr && size != 0u) || size > capacity) {
        return false;
    }
    for (uint32_t index = 0u; index < size; ++index) {
        if (bytes[index] == 0u) {
            return false;
        }
    }
    return true;
}

std::string bytes_to_string(const uint8_t *bytes, uint32_t size) {
    return std::string(reinterpret_cast<const char *>(bytes), static_cast<size_t>(size));
}

template <size_t DestinationCapacity>
bool copy_vendor_text(
    char (&destination)[DestinationCapacity],
    const std::string &source
) noexcept {
    if (source.size() >= DestinationCapacity) {
        return false;
    }
    std::memset(destination, 0, DestinationCapacity);
    if (!source.empty()) {
        std::memcpy(destination, source.data(), source.size());
    }
    return true;
}

template <size_t SourceCapacity, size_t DestinationCapacity>
void copy_event_text(
    const char (&source)[SourceCapacity],
    uint8_t (&destination)[DestinationCapacity],
    uint32_t *out_size
) noexcept {
    const size_t source_size = bounded_text_length(source, SourceCapacity);
    const size_t copy_size = std::min(source_size, DestinationCapacity);
    std::memset(destination, 0, DestinationCapacity);
    if (copy_size != 0u) {
        std::memcpy(destination, source, copy_size);
    }
    *out_size = static_cast<uint32_t>(copy_size);
}

template <size_t DestinationCapacity>
void copy_literal_text(
    const char *source,
    uint8_t (&destination)[DestinationCapacity],
    uint32_t *out_size
) noexcept {
    const size_t source_size = source == nullptr ? 0u : std::strlen(source);
    const size_t copy_size = std::min(source_size, DestinationCapacity);
    std::memset(destination, 0, DestinationCapacity);
    if (copy_size != 0u) {
        std::memcpy(destination, source, copy_size);
    }
    *out_size = static_cast<uint32_t>(copy_size);
}

const char *exchange_name(TTORATstpExchangeIDType exchange) noexcept {
    switch (exchange) {
        case TORA_TSTP_EXD_SSE:
            return "SSE";
        case TORA_TSTP_EXD_SZSE:
            return "SZSE";
        case TORA_TSTP_EXD_HK:
            return "HK";
        case TORA_TSTP_EXD_BSE:
            return "BSE";
        default:
            return "";
    }
}

bool parse_exchange(const uint8_t *bytes, uint32_t size, TTORATstpExchangeIDType *out) {
    if (out == nullptr || !bytes_are_c_text(bytes, size, BT_HUAXIN_EXCHANGE_CAPACITY)) {
        return false;
    }
    const std::string value = bytes_to_string(bytes, size);
    if (value == "SSE" || value == "SH" || value == "XSHG" || value == "1") {
        *out = TORA_TSTP_EXD_SSE;
        return true;
    }
    if (value == "SZSE" || value == "SZ" || value == "XSHE" || value == "2") {
        *out = TORA_TSTP_EXD_SZSE;
        return true;
    }
    if (value == "BSE" || value == "BJ" || value == "XBEI" || value == "4") {
        *out = TORA_TSTP_EXD_BSE;
        return true;
    }
    if (value.empty()) {
        *out = TORA_TSTP_EXD_COMM;
        return true;
    }
    return false;
}

uint8_t normalized_direction(TTORATstpDirectionType direction) noexcept {
    if (direction == TORA_TSTP_D_Buy) {
        return 0u;
    }
    if (direction == TORA_TSTP_D_Sell) {
        return 1u;
    }
    return static_cast<uint8_t>(direction);
}

uint8_t normalized_order_price_type(TTORATstpOrderPriceTypeType value) noexcept {
    switch (value) {
        case TORA_TSTP_OPT_LimitPrice:
            return BT_HUAXIN_ORDER_PRICE_LIMIT;
        case TORA_TSTP_OPT_HomeBestPrice:
            return BT_HUAXIN_ORDER_PRICE_HOME_BEST;
        case TORA_TSTP_OPT_BestPrice:
            return BT_HUAXIN_ORDER_PRICE_OPPONENT_BEST;
        case TORA_TSTP_OPT_FiveLevelPrice:
            return BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL;
        case TORA_TSTP_OPT_AnyPrice:
            return BT_HUAXIN_ORDER_PRICE_ANY;
        default:
            return static_cast<uint8_t>(value);
    }
}

uint8_t normalized_time_condition(TTORATstpTimeConditionType value) noexcept {
    switch (value) {
        case TORA_TSTP_TC_GFD:
            return BT_HUAXIN_TIME_GFD;
        case TORA_TSTP_TC_IOC:
            return BT_HUAXIN_TIME_IOC;
        default:
            return static_cast<uint8_t>(value);
    }
}

uint8_t normalized_volume_condition(TTORATstpVolumeConditionType value) noexcept {
    switch (value) {
        case TORA_TSTP_VC_AV:
            return BT_HUAXIN_VOLUME_ANY;
        case TORA_TSTP_VC_CV:
            return BT_HUAXIN_VOLUME_ALL;
        default:
            return static_cast<uint8_t>(value);
    }
}

bool ready_for_new_orders_locked(const bt_huaxin_handle *handle) noexcept {
    return handle->logged_in && handle->session.enable_trading &&
           handle->initial_query_mask == kInitialQueryMask;
}

bool ready_for_cancel_locked(const bt_huaxin_handle *handle) noexcept {
    return handle->logged_in && handle->session.enable_cancel;
}

int32_t push_owned_event_locked(
    bt_huaxin_handle *handle,
    uint32_t event_type,
    uint64_t request_id,
    const void *payload,
    uint32_t payload_size
) noexcept {
    if (handle == nullptr || payload_size > BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY ||
        (payload == nullptr && payload_size != 0u)) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    if (handle->events.size() >= handle->queue_capacity) {
        ++handle->dropped_events;
        return BT_HUAXIN_QUEUE_FULL;
    }
    try {
        bt_huaxin_owned_event event{};
        event.abi_version = BT_HUAXIN_ABI_VERSION;
        event.struct_size = static_cast<uint32_t>(sizeof(event));
        event.event_type = event_type;
        event.payload_size = payload_size;
        event.sequence = handle->next_sequence++;
        event.received_ns = monotonic_nanoseconds();
        event.request_id = request_id;
        set_schema_identity(&event.schema);
        if (payload_size != 0u) {
            std::memcpy(event.payload, payload, payload_size);
        }
        handle->events.push_back(event);
        return BT_HUAXIN_OK;
    } catch (const std::bad_alloc &) {
        return BT_HUAXIN_ALLOCATION_FAILED;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

void emit_state_locked(bt_huaxin_handle *handle, int32_t reason) noexcept {
    bt_huaxin_state_event event{};
    event.state = handle->state;
    event.reason = reason;
    event.transport_connected = handle->transport_connected ? 1u : 0u;
    event.logged_in = handle->logged_in ? 1u : 0u;
    event.ready_for_queries = handle->ready_for_queries ? 1u : 0u;
    event.ready_for_new_orders = ready_for_new_orders_locked(handle) ? 1u : 0u;
    event.ready_for_cancel = ready_for_cancel_locked(handle) ? 1u : 0u;
    event.session_epoch = handle->session_epoch;
    (void)push_owned_event_locked(
        handle,
        BT_HUAXIN_EVENT_STATE,
        0u,
        &event,
        static_cast<uint32_t>(sizeof(event))
    );
}

void emit_error_locked(
    bt_huaxin_handle *handle,
    uint64_t request_id,
    int32_t error_id,
    int32_t vendor_request_id,
    const char *message,
    size_t message_capacity
) noexcept {
    bt_huaxin_error_event event{};
    event.error_id = error_id;
    event.vendor_request_id = vendor_request_id;
    const size_t message_size = bounded_text_length(message, message_capacity);
    const size_t copy_size = std::min(
        message_size,
        static_cast<size_t>(BT_HUAXIN_ERROR_MESSAGE_CAPACITY)
    );
    event.message_size = static_cast<uint32_t>(copy_size);
    if (copy_size != 0u) {
        std::memcpy(event.message, message, copy_size);
    }
    handle->last_error_id = error_id;
    (void)push_owned_event_locked(
        handle,
        BT_HUAXIN_EVENT_ERROR,
        request_id,
        &event,
        static_cast<uint32_t>(sizeof(event))
    );
}

int32_t response_error_id(const CTORATstpRspInfoField *info) noexcept {
    return info == nullptr ? 0 : info->ErrorID;
}

const char *response_error_message(const CTORATstpRspInfoField *info) noexcept {
    return info == nullptr ? "" : info->ErrorMsg;
}

void finish_query_locked(
    bt_huaxin_handle *handle,
    int request_id,
    bool is_last,
    const CTORATstpRspInfoField *info
) noexcept {
    if (!is_last) {
        return;
    }
    const auto found = handle->queries.find(request_id);
    const uint32_t request_type = found == handle->queries.end()
        ? 0u
        : found->second.request_type;
    const uint32_t record_count = found == handle->queries.end()
        ? 0u
        : found->second.record_count;
    const int32_t error_id = response_error_id(info);
    bt_huaxin_query_end_event event{};
    event.request_type = request_type;
    event.error_id = error_id;
    event.record_count = record_count;
    const char *message = response_error_message(info);
    const size_t message_size = bounded_text_length(message, sizeof(info->ErrorMsg));
    const size_t copy_size = std::min(
        message_size,
        static_cast<size_t>(BT_HUAXIN_ERROR_MESSAGE_CAPACITY)
    );
    event.message_size = static_cast<uint32_t>(copy_size);
    if (copy_size != 0u) {
        std::memcpy(event.message, message, copy_size);
    }
    (void)push_owned_event_locked(
        handle,
        BT_HUAXIN_EVENT_QUERY_END,
        static_cast<uint64_t>(request_id),
        &event,
        static_cast<uint32_t>(sizeof(event))
    );
    if (error_id == 0) {
        switch (request_type) {
            case BT_HUAXIN_REQUEST_QUERY_SHAREHOLDER_ACCOUNT:
                handle->initial_query_mask |= kInitialShareholder;
                break;
            case BT_HUAXIN_REQUEST_QUERY_TRADING_ACCOUNT:
                handle->initial_query_mask |= kInitialAccount;
                break;
            case BT_HUAXIN_REQUEST_QUERY_POSITION:
                handle->initial_query_mask |= kInitialPosition;
                break;
            case BT_HUAXIN_REQUEST_QUERY_ORDER:
                handle->initial_query_mask |= kInitialOrder;
                break;
            case BT_HUAXIN_REQUEST_QUERY_TRADE:
                handle->initial_query_mask |= kInitialTrade;
                break;
            default:
                break;
        }
        if (handle->initial_query_mask == kInitialQueryMask &&
            handle->state == BT_HUAXIN_STATE_LOGGED_IN) {
            handle->state = BT_HUAXIN_STATE_READY_READ_ONLY;
            emit_state_locked(handle, 0);
        }
    } else {
        handle->last_error_id = error_id;
    }
    handle->queries.erase(request_id);
}

void increment_query_count_locked(bt_huaxin_handle *handle, int request_id) noexcept {
    const auto found = handle->queries.find(request_id);
    if (found != handle->queries.end()) {
        ++found->second.record_count;
    }
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

bool claim_batch_allocation(uint64_t token, BatchAllocation *out) noexcept {
    try {
        if (token == 0u || out == nullptr) {
            return false;
        }
        std::lock_guard<std::mutex> lock(g_batch_allocation_mutex);
        const auto found = g_batch_allocations.find(token);
        if (found == g_batch_allocations.end()) {
            return false;
        }
        *out = found->second;
        g_batch_allocations.erase(found);
        return true;
    } catch (...) {
        return false;
    }
}

bool valid_topic(int32_t topic, bool allow_disabled) noexcept {
    return (allow_disabled && topic == BT_HUAXIN_TOPIC_DISABLED) ||
           topic == BT_HUAXIN_TOPIC_RESTART ||
           topic == BT_HUAXIN_TOPIC_RESUME ||
           topic == BT_HUAXIN_TOPIC_QUICK;
}

int32_t validate_session_config(const bt_huaxin_session_config *config) noexcept {
    if (config == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        config->abi_version,
        config->struct_size,
        sizeof(*config)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    if (!schema_identity_matches(config->schema)) {
        return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
    }
    if (config->reserved_flags != 0u || config->encrypt > 1u ||
        config->enable_trading > 1u || config->enable_cancel > 1u ||
        config->login_account_type < BT_HUAXIN_LOGIN_USER_ID ||
        config->login_account_type > BT_HUAXIN_LOGIN_BJ_STOCK ||
        (config->trade_comm_mode != BT_HUAXIN_TRADE_COMM_TCP &&
         config->trade_comm_mode != BT_HUAXIN_TRADE_COMM_TCP_DIRECT) ||
        !valid_topic(config->private_topic, false) ||
        !valid_topic(config->public_topic, true)) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const bool public_lengths_valid =
        bytes_are_c_text(
            config->flow_path,
            config->flow_path_size,
            BT_HUAXIN_FLOW_PATH_CAPACITY
        ) &&
        bytes_are_c_text(
            config->trade_front,
            config->trade_front_size,
            BT_HUAXIN_FRONT_CAPACITY
        ) &&
        bytes_are_c_text(
            config->login_account,
            config->login_account_size,
            BT_HUAXIN_LOGIN_ACCOUNT_CAPACITY
        ) &&
        bytes_are_c_text(
            config->department_id,
            config->department_id_size,
            BT_HUAXIN_DEPARTMENT_CAPACITY
        ) &&
        bytes_are_c_text(
            config->password,
            config->password_size,
            BT_HUAXIN_PASSWORD_CAPACITY
        ) &&
        bytes_are_c_text(
            config->dynamic_password,
            config->dynamic_password_size,
            BT_HUAXIN_PASSWORD_CAPACITY
        ) &&
        bytes_are_c_text(
            config->user_product_info,
            config->user_product_info_size,
            BT_HUAXIN_USER_PRODUCT_INFO_CAPACITY
        ) &&
        bytes_are_c_text(
            config->interface_product_info,
            config->interface_product_info_size,
            BT_HUAXIN_INTERFACE_PRODUCT_INFO_CAPACITY
        ) &&
        bytes_are_c_text(
            config->terminal_info,
            config->terminal_info_size,
            BT_HUAXIN_TERMINAL_INFO_CAPACITY
        ) &&
        bytes_are_c_text(
            config->mac_address,
            config->mac_address_size,
            BT_HUAXIN_MAC_ADDRESS_CAPACITY
        ) &&
        bytes_are_c_text(
            config->interface_address,
            config->interface_address_size,
            BT_HUAXIN_INTERFACE_ADDRESS_CAPACITY
        );
    if (!public_lengths_valid || config->flow_path_size == 0u ||
        config->trade_front_size == 0u || config->login_account_size == 0u ||
        config->password_size == 0u || config->user_product_info_size == 0u ||
        config->terminal_info_size == 0u || config->mac_address_size == 0u) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    if (config->login_account_size >= sizeof(CTORATstpReqUserLoginField{}.LogInAccount) ||
        config->department_id_size >= sizeof(CTORATstpReqUserLoginField{}.DepartmentID) ||
        config->password_size >= sizeof(CTORATstpReqUserLoginField{}.Password) ||
        config->dynamic_password_size >= sizeof(CTORATstpReqUserLoginField{}.DynamicPassword) ||
        config->user_product_info_size >= sizeof(CTORATstpReqUserLoginField{}.UserProductInfo) ||
        config->interface_product_info_size >= sizeof(CTORATstpReqUserLoginField{}.InterfaceProductInfo) ||
        config->terminal_info_size >= sizeof(CTORATstpReqUserLoginField{}.TerminalInfo) ||
        config->mac_address_size >= sizeof(CTORATstpReqUserLoginField{}.MacAddress)) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    return BT_HUAXIN_OK;
}

SessionValues copy_session_values(const bt_huaxin_session_config &config) {
    SessionValues values;
    values.flow_path = bytes_to_string(config.flow_path, config.flow_path_size);
    values.trade_front = bytes_to_string(config.trade_front, config.trade_front_size);
    values.login_account = bytes_to_string(config.login_account, config.login_account_size);
    values.department_id = bytes_to_string(config.department_id, config.department_id_size);
    values.password = bytes_to_string(config.password, config.password_size);
    values.dynamic_password = bytes_to_string(config.dynamic_password, config.dynamic_password_size);
    values.user_product_info = bytes_to_string(
        config.user_product_info,
        config.user_product_info_size
    );
    values.interface_product_info = bytes_to_string(
        config.interface_product_info,
        config.interface_product_info_size
    );
    values.terminal_info = bytes_to_string(config.terminal_info, config.terminal_info_size);
    values.mac_address = bytes_to_string(config.mac_address, config.mac_address_size);
    values.interface_address = bytes_to_string(
        config.interface_address,
        config.interface_address_size
    );
    values.login_account_type = config.login_account_type;
    values.trade_comm_mode = config.trade_comm_mode;
    values.private_topic = config.private_topic;
    values.public_topic = config.public_topic;
    values.encrypt = config.encrypt != 0u;
    values.enable_trading = config.enable_trading != 0u;
    values.enable_cancel = config.enable_cancel != 0u;
    return values;
}

TTORATstpLogInAccountTypeType login_account_type_value(int32_t value) noexcept {
    if (value == BT_HUAXIN_LOGIN_BJ_STOCK) {
        return TORA_TSTP_LACT_BJAStock;
    }
    return static_cast<TTORATstpLogInAccountTypeType>('0' + value);
}

TORA_TE_RESUME_TYPE topic_value(int32_t value) noexcept {
    return static_cast<TORA_TE_RESUME_TYPE>(value);
}

TTORATstpTradeCommModeType trade_comm_value(int32_t value) noexcept {
    return value == BT_HUAXIN_TRADE_COMM_TCP_DIRECT
        ? TORA_TSTP_TCM_TCPDIRECT
        : TORA_TSTP_TCM_TCP;
}

void emit_vendor_call_error(
    bt_huaxin_handle *handle,
    uint64_t request_id,
    int vendor_request_id,
    int vendor_result
) noexcept {
    std::lock_guard<std::mutex> lock(handle->mutex);
    emit_error_locked(handle, request_id, vendor_result, vendor_request_id, "vendor request rejected", 23u);
}

}  // namespace

void TraderSpi::OnFrontConnected() {
    CTORATstpTraderApi *api = nullptr;
    {
        std::lock_guard<std::mutex> lock(handle_->mutex);
        handle_->transport_connected = true;
        handle_->state = BT_HUAXIN_STATE_FRONT_CONNECTED;
        emit_state_locked(handle_, 0);
        api = handle_->api;
    }
    if (api == nullptr) {
        return;
    }
    constexpr int kConnectionRequestId = -1000000001;
    const int result = api->ReqGetConnectionInfo(kConnectionRequestId);
    if (result != 0) {
        emit_vendor_call_error(handle_, 0u, kConnectionRequestId, result);
    }
}

void TraderSpi::OnFrontDisconnected(int reason) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    handle_->transport_connected = false;
    handle_->logged_in = false;
    handle_->ready_for_queries = false;
    handle_->initial_query_mask = 0u;
    handle_->queries.clear();
    handle_->state = BT_HUAXIN_STATE_DISCONNECTED;
    emit_state_locked(handle_, reason);
}

void TraderSpi::OnRspError(
    CTORATstpRspInfoField *info,
    int request_id,
    bool is_last
) {
    (void)is_last;
    std::lock_guard<std::mutex> lock(handle_->mutex);
    emit_error_locked(
        handle_,
        request_id > 0 ? static_cast<uint64_t>(request_id) : 0u,
        response_error_id(info),
        request_id,
        response_error_message(info),
        info == nullptr ? 0u : sizeof(info->ErrorMsg)
    );
}

void TraderSpi::OnRspGetConnectionInfo(
    CTORATstpConnectionInfoField *connection,
    CTORATstpRspInfoField *info,
    int request_id
) {
    (void)connection;
    CTORATstpTraderApi *api = nullptr;
    SessionValues session;
    {
        std::lock_guard<std::mutex> lock(handle_->mutex);
        const int32_t error_id = response_error_id(info);
        if (error_id != 0) {
            emit_error_locked(
                handle_,
                0u,
                error_id,
                request_id,
                response_error_message(info),
                info == nullptr ? 0u : sizeof(info->ErrorMsg)
            );
            handle_->state = BT_HUAXIN_STATE_FAULTED;
            emit_state_locked(handle_, error_id);
            return;
        }
        api = handle_->api;
        session = handle_->session;
        handle_->state = BT_HUAXIN_STATE_LOGIN_PENDING;
        emit_state_locked(handle_, 0);
    }
    if (api == nullptr) {
        return;
    }
    CTORATstpReqUserLoginField login{};
    constexpr int kLoginRequestId = -1000000002;
    login.UserRequestID = kLoginRequestId;
    login.LogInAccountType = login_account_type_value(session.login_account_type);
    login.AuthMode = TORA_TSTP_AM_Password;
    login.Lang = TORA_TSTP_LGT_ZHCN;
    if (!copy_vendor_text(login.LogInAccount, session.login_account) ||
        !copy_vendor_text(login.DepartmentID, session.department_id) ||
        !copy_vendor_text(login.Password, session.password) ||
        !copy_vendor_text(login.DynamicPassword, session.dynamic_password) ||
        !copy_vendor_text(login.UserProductInfo, session.user_product_info) ||
        !copy_vendor_text(login.InterfaceProductInfo, session.interface_product_info) ||
        !copy_vendor_text(login.TerminalInfo, session.terminal_info)) {
        secure_clear_bytes(&login, sizeof(login));
        emit_vendor_call_error(handle_, 0u, kLoginRequestId, BT_HUAXIN_INVALID_ARGUMENT);
        return;
    }
    const int result = api->ReqUserLogin(&login, kLoginRequestId);
    secure_clear_bytes(&login, sizeof(login));
    if (result != 0) {
        emit_vendor_call_error(handle_, 0u, kLoginRequestId, result);
    }
}

void TraderSpi::OnRspUserLogin(
    CTORATstpRspUserLoginField *login,
    CTORATstpRspInfoField *info,
    int request_id
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    const int32_t error_id = response_error_id(info);
    if (error_id != 0 || login == nullptr) {
        emit_error_locked(
            handle_,
            0u,
            error_id == 0 ? BT_HUAXIN_INTERNAL_ERROR : error_id,
            request_id,
            response_error_message(info),
            info == nullptr ? 0u : sizeof(info->ErrorMsg)
        );
        handle_->state = BT_HUAXIN_STATE_FAULTED;
        emit_state_locked(handle_, error_id);
        return;
    }
    handle_->front_id = login->FrontID;
    handle_->session_id = login->SessionID;
    handle_->max_order_ref = login->MaxOrderRef;
    handle_->logged_in = true;
    handle_->ready_for_queries = true;
    handle_->initial_query_mask = 0u;
    ++handle_->session_epoch;
    handle_->state = BT_HUAXIN_STATE_LOGGED_IN;
    bt_huaxin_login_event event{};
    event.front_id = login->FrontID;
    event.session_id = login->SessionID;
    event.max_order_ref = login->MaxOrderRef;
    copy_event_text(login->TradingDay, event.trading_day, &event.trading_day_size);
    copy_event_text(login->LoginTime, event.login_time, &event.login_time_size);
    (void)push_owned_event_locked(
        handle_,
        BT_HUAXIN_EVENT_LOGIN,
        0u,
        &event,
        static_cast<uint32_t>(sizeof(event))
    );
    emit_state_locked(handle_, 0);
}

void TraderSpi::OnRspQrySecurity(
    CTORATstpSecurityField *field,
    CTORATstpRspInfoField *info,
    int request_id,
    bool is_last
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    if (field != nullptr) {
        bt_huaxin_security_event event{};
        copy_literal_text(exchange_name(field->ExchangeID), event.exchange, &event.exchange_size);
        copy_event_text(field->SecurityID, event.security, &event.security_size);
        copy_event_text(field->SecurityName, event.security_name, &event.security_name_size);
        copy_event_text(field->ShortSecurityName, event.short_name, &event.short_name_size);
        event.market_id = static_cast<unsigned char>(field->MarketID);
        event.security_type = static_cast<unsigned char>(field->SecurityType);
        event.order_unit = static_cast<unsigned char>(field->OrderUnit);
        event.limit_buy_unit = field->LimitBuyTradingUnit;
        event.limit_sell_unit = field->LimitSellTradingUnit;
        event.min_limit_buy = field->MinLimitOrderBuyVolume;
        event.max_limit_buy = field->MaxLimitOrderBuyVolume;
        event.min_limit_sell = field->MinLimitOrderSellVolume;
        event.max_limit_sell = field->MaxLimitOrderSellVolume;
        event.market_buy_unit = field->MarketBuyTradingUnit;
        event.market_sell_unit = field->MarketSellTradingUnit;
        event.min_market_buy = field->MinMarketOrderBuyVolume;
        event.max_market_buy = field->MaxMarketOrderBuyVolume;
        event.min_market_sell = field->MinMarketOrderSellVolume;
        event.max_market_sell = field->MaxMarketOrderSellVolume;
        event.volume_multiple = field->VolumeMultiple;
        event.has_price_limit = field->bPriceLimit == 0 ? 0u : 1u;
        event.day_trading = field->DayTrading == 0 ? 0u : 1u;
        event.security_status = static_cast<int64_t>(field->SecurityStatus);
        event.price_tick = field->PriceTick;
        event.pre_close_price = field->PreClosePrice;
        event.upper_limit_price = field->UpperLimitPrice;
        event.lower_limit_price = field->LowerLimitPrice;
        (void)push_owned_event_locked(
            handle_,
            BT_HUAXIN_EVENT_SECURITY,
            static_cast<uint64_t>(request_id),
            &event,
            static_cast<uint32_t>(sizeof(event))
        );
        increment_query_count_locked(handle_, request_id);
    }
    finish_query_locked(handle_, request_id, is_last, info);
}

void TraderSpi::OnRspQryShareholderAccount(
    CTORATstpShareholderAccountField *field,
    CTORATstpRspInfoField *info,
    int request_id,
    bool is_last
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    if (field != nullptr) {
        bt_huaxin_shareholder_event event{};
        copy_event_text(field->InvestorID, event.investor_id, &event.investor_id_size);
        copy_literal_text(exchange_name(field->ExchangeID), event.exchange, &event.exchange_size);
        copy_event_text(field->ShareholderID, event.shareholder_id, &event.shareholder_id_size);
        event.market_id = static_cast<unsigned char>(field->MarketID);
        event.shareholder_id_type = static_cast<unsigned char>(field->ShareholderIDType);
        event.main_flag = field->MainFlag ? 1u : 0u;
        (void)push_owned_event_locked(
            handle_,
            BT_HUAXIN_EVENT_SHAREHOLDER_ACCOUNT,
            static_cast<uint64_t>(request_id),
            &event,
            static_cast<uint32_t>(sizeof(event))
        );
        increment_query_count_locked(handle_, request_id);
    }
    finish_query_locked(handle_, request_id, is_last, info);
}

void TraderSpi::OnRspQryTradingAccount(
    CTORATstpTradingAccountField *field,
    CTORATstpRspInfoField *info,
    int request_id,
    bool is_last
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    if (field != nullptr) {
        bt_huaxin_account_event event{};
        copy_event_text(field->DepartmentID, event.department_id, &event.department_id_size);
        copy_event_text(field->AccountID, event.account_id, &event.account_id_size);
        event.currency = static_cast<unsigned char>(field->CurrencyID);
        event.available_cash = field->UsefulMoney;
        event.transferable_cash = field->FetchLimit;
        event.frozen_cash = field->FrozenCash;
        (void)push_owned_event_locked(
            handle_,
            BT_HUAXIN_EVENT_TRADING_ACCOUNT,
            static_cast<uint64_t>(request_id),
            &event,
            static_cast<uint32_t>(sizeof(event))
        );
        increment_query_count_locked(handle_, request_id);
    }
    finish_query_locked(handle_, request_id, is_last, info);
}

void TraderSpi::OnRspQryPosition(
    CTORATstpPositionField *field,
    CTORATstpRspInfoField *info,
    int request_id,
    bool is_last
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    if (field != nullptr) {
        bt_huaxin_position_event event{};
        copy_literal_text(exchange_name(field->ExchangeID), event.exchange, &event.exchange_size);
        copy_event_text(field->InvestorID, event.investor_id, &event.investor_id_size);
        copy_event_text(field->ShareholderID, event.shareholder_id, &event.shareholder_id_size);
        copy_event_text(field->SecurityID, event.security, &event.security_size);
        copy_event_text(field->TradingDay, event.trading_day, &event.trading_day_size);
        copy_position_numeric_fields(*field, &event);
        (void)push_owned_event_locked(
            handle_,
            BT_HUAXIN_EVENT_POSITION,
            static_cast<uint64_t>(request_id),
            &event,
            static_cast<uint32_t>(sizeof(event))
        );
        increment_query_count_locked(handle_, request_id);
    }
    finish_query_locked(handle_, request_id, is_last, info);
}

namespace {

bt_huaxin_order_event make_order_event(const CTORATstpOrderField &field) noexcept {
    bt_huaxin_order_event event{};
    copy_literal_text(exchange_name(field.ExchangeID), event.exchange, &event.exchange_size);
    copy_event_text(field.InvestorID, event.investor_id, &event.investor_id_size);
    copy_event_text(field.ShareholderID, event.shareholder_id, &event.shareholder_id_size);
    copy_event_text(field.SecurityID, event.security, &event.security_size);
    event.direction = normalized_direction(field.Direction);
    event.order_price_type = normalized_order_price_type(field.OrderPriceType);
    event.time_condition = normalized_time_condition(field.TimeCondition);
    event.volume_condition = normalized_volume_condition(field.VolumeCondition);
    event.order_status = static_cast<uint8_t>(field.OrderStatus);
    event.submit_status = static_cast<uint8_t>(field.OrderSubmitStatus);
    event.limit_price = field.LimitPrice;
    event.amount = field.VolumeTotalOriginal;
    event.filled = field.VolumeTraded;
    event.canceled = field.VolumeCanceled;
    event.front_id = field.FrontID;
    event.session_id = field.SessionID;
    event.order_ref = field.OrderRef;
    copy_event_text(field.OrderLocalID, event.order_local_id, &event.order_local_id_size);
    copy_event_text(field.OrderSysID, event.order_sys_id, &event.order_sys_id_size);
    copy_event_text(field.TradingDay, event.trading_day, &event.trading_day_size);
    copy_event_text(field.InsertTime, event.insert_time, &event.insert_time_size);
    copy_event_text(field.StatusMsg, event.status_message, &event.status_message_size);
    return event;
}

bt_huaxin_trade_event make_trade_event(const CTORATstpTradeField &field) noexcept {
    bt_huaxin_trade_event event{};
    copy_literal_text(exchange_name(field.ExchangeID), event.exchange, &event.exchange_size);
    copy_event_text(field.InvestorID, event.investor_id, &event.investor_id_size);
    copy_event_text(field.ShareholderID, event.shareholder_id, &event.shareholder_id_size);
    copy_event_text(field.SecurityID, event.security, &event.security_size);
    event.direction = normalized_direction(field.Direction);
    copy_event_text(field.TradeID, event.trade_id, &event.trade_id_size);
    copy_event_text(field.OrderSysID, event.order_sys_id, &event.order_sys_id_size);
    copy_event_text(field.OrderLocalID, event.order_local_id, &event.order_local_id_size);
    event.order_ref = field.OrderRef;
    event.price = field.Price;
    event.amount = field.Volume;
    copy_event_text(field.TradeDate, event.trade_date, &event.trade_date_size);
    copy_event_text(field.TradeTime, event.trade_time, &event.trade_time_size);
    copy_event_text(field.TradingDay, event.trading_day, &event.trading_day_size);
    return event;
}

void emit_order_response_locked(
    bt_huaxin_handle *handle,
    uint32_t event_type,
    int request_id,
    int order_ref,
    const char *order_sys_id,
    size_t order_sys_capacity,
    const CTORATstpRspInfoField *info
) noexcept {
    bt_huaxin_order_response_event event{};
    event.error_id = response_error_id(info);
    event.order_ref = order_ref;
    const size_t sys_size = bounded_text_length(order_sys_id, order_sys_capacity);
    event.order_sys_id_size = static_cast<uint32_t>(
        std::min(sys_size, static_cast<size_t>(BT_HUAXIN_ORDER_SYS_ID_CAPACITY))
    );
    if (event.order_sys_id_size != 0u) {
        std::memcpy(event.order_sys_id, order_sys_id, event.order_sys_id_size);
    }
    const char *message = response_error_message(info);
    const size_t message_size = bounded_text_length(
        message,
        info == nullptr ? 0u : sizeof(info->ErrorMsg)
    );
    event.message_size = static_cast<uint32_t>(
        std::min(message_size, static_cast<size_t>(BT_HUAXIN_ERROR_MESSAGE_CAPACITY))
    );
    if (event.message_size != 0u) {
        std::memcpy(event.message, message, event.message_size);
    }
    (void)push_owned_event_locked(
        handle,
        event_type,
        request_id > 0 ? static_cast<uint64_t>(request_id) : 0u,
        &event,
        static_cast<uint32_t>(sizeof(event))
    );
}

}  // namespace

void TraderSpi::OnRspQryOrder(
    CTORATstpOrderField *field,
    CTORATstpRspInfoField *info,
    int request_id,
    bool is_last
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    if (field != nullptr) {
        const bt_huaxin_order_event event = make_order_event(*field);
        (void)push_owned_event_locked(
            handle_,
            BT_HUAXIN_EVENT_ORDER,
            static_cast<uint64_t>(request_id),
            &event,
            static_cast<uint32_t>(sizeof(event))
        );
        increment_query_count_locked(handle_, request_id);
    }
    finish_query_locked(handle_, request_id, is_last, info);
}

void TraderSpi::OnRspQryTrade(
    CTORATstpTradeField *field,
    CTORATstpRspInfoField *info,
    int request_id,
    bool is_last
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    if (field != nullptr) {
        const bt_huaxin_trade_event event = make_trade_event(*field);
        (void)push_owned_event_locked(
            handle_,
            BT_HUAXIN_EVENT_TRADE,
            static_cast<uint64_t>(request_id),
            &event,
            static_cast<uint32_t>(sizeof(event))
        );
        increment_query_count_locked(handle_, request_id);
    }
    finish_query_locked(handle_, request_id, is_last, info);
}

void TraderSpi::OnRspOrderInsert(
    CTORATstpInputOrderField *field,
    CTORATstpRspInfoField *info,
    int request_id
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    emit_order_response_locked(
        handle_,
        BT_HUAXIN_EVENT_ORDER_INSERT_RESPONSE,
        request_id,
        field == nullptr ? 0 : field->OrderRef,
        field == nullptr ? "" : field->OrderSysID,
        field == nullptr ? 0u : sizeof(field->OrderSysID),
        info
    );
}

void TraderSpi::OnErrRtnOrderInsert(
    CTORATstpInputOrderField *field,
    CTORATstpRspInfoField *info,
    int request_id
) {
    OnRspOrderInsert(field, info, request_id);
}

void TraderSpi::OnRspOrderAction(
    CTORATstpInputOrderActionField *field,
    CTORATstpRspInfoField *info,
    int request_id
) {
    std::lock_guard<std::mutex> lock(handle_->mutex);
    emit_order_response_locked(
        handle_,
        BT_HUAXIN_EVENT_ORDER_ACTION_RESPONSE,
        request_id,
        field == nullptr ? 0 : field->OrderRef,
        field == nullptr ? "" : field->OrderSysID,
        field == nullptr ? 0u : sizeof(field->OrderSysID),
        info
    );
}

void TraderSpi::OnErrRtnOrderAction(
    CTORATstpInputOrderActionField *field,
    CTORATstpRspInfoField *info,
    int request_id
) {
    OnRspOrderAction(field, info, request_id);
}

void TraderSpi::OnRtnOrder(CTORATstpOrderField *field) {
    if (field == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(handle_->mutex);
    const bt_huaxin_order_event event = make_order_event(*field);
    (void)push_owned_event_locked(
        handle_,
        BT_HUAXIN_EVENT_ORDER,
        field->RequestID > 0 ? static_cast<uint64_t>(field->RequestID) : 0u,
        &event,
        static_cast<uint32_t>(sizeof(event))
    );
}

void TraderSpi::OnRtnTrade(CTORATstpTradeField *field) {
    if (field == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(handle_->mutex);
    const bt_huaxin_trade_event event = make_trade_event(*field);
    (void)push_owned_event_locked(
        handle_,
        BT_HUAXIN_EVENT_TRADE,
        0u,
        &event,
        static_cast<uint32_t>(sizeof(event))
    );
}

namespace {

int32_t stop_session(bt_huaxin_handle *handle) noexcept {
    CTORATstpTraderApi *api = nullptr;
    TraderSpi *spi = nullptr;
    {
        std::lock_guard<std::mutex> lock(handle->mutex);
        api = handle->api;
        spi = handle->spi;
        handle->api = nullptr;
        handle->spi = nullptr;
        handle->transport_connected = false;
        handle->logged_in = false;
        handle->ready_for_queries = false;
        handle->initial_query_mask = 0u;
        handle->queries.clear();
        handle->session.clear_sensitive();
        handle->state = BT_HUAXIN_STATE_CLOSED;
        emit_state_locked(handle, 0);
    }
    if (api != nullptr) {
        try {
            api->RegisterSpi(nullptr);
            api->Release();
        } catch (...) {
            delete spi;
            return BT_HUAXIN_INTERNAL_ERROR;
        }
    }
    delete spi;
    return BT_HUAXIN_OK;
}

int32_t validate_request(const bt_huaxin_request *request) noexcept {
    if (request == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        request->abi_version,
        request->struct_size,
        sizeof(*request)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    if (!schema_identity_matches(request->schema)) {
        return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
    }
    if (request->request_id == 0u || request->request_id > static_cast<uint64_t>(INT_MAX) ||
        request->payload_size > BT_HUAXIN_REQUEST_PAYLOAD_CAPACITY) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    return BT_HUAXIN_OK;
}

constexpr bool valid_limit_order_amount(uint32_t amount) noexcept {
    return amount >= 1u &&
           amount <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max());
}

static_assert(!valid_limit_order_amount(0u));
static_assert(valid_limit_order_amount(1u));
static_assert(
    valid_limit_order_amount(static_cast<uint32_t>(std::numeric_limits<int32_t>::max()))
);
static_assert(
    !valid_limit_order_amount(
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) + 1u
    )
);
static_assert(!valid_limit_order_amount(std::numeric_limits<uint32_t>::max()));

struct VendorOrderSemantics {
    TTORATstpOrderPriceTypeType order_price_type = 0;
    TTORATstpTimeConditionType time_condition = 0;
    TTORATstpVolumeConditionType volume_condition = 0;
    bool supported = false;
};

constexpr VendorOrderSemantics map_order_semantics(
    TTORATstpExchangeIDType exchange,
    uint8_t order_price_type,
    uint8_t time_condition,
    uint8_t volume_condition
) noexcept {
    const bool is_sse = exchange == TORA_TSTP_EXD_SSE;
    const bool is_szse = exchange == TORA_TSTP_EXD_SZSE;
    const bool is_bse = exchange == TORA_TSTP_EXD_BSE;
    if (order_price_type == BT_HUAXIN_ORDER_PRICE_LIMIT &&
        time_condition == BT_HUAXIN_TIME_GFD &&
        volume_condition == BT_HUAXIN_VOLUME_ANY &&
        (is_sse || is_szse || is_bse)) {
        return {
            TORA_TSTP_OPT_LimitPrice,
            TORA_TSTP_TC_GFD,
            TORA_TSTP_VC_AV,
            true,
        };
    }
    if (order_price_type == BT_HUAXIN_ORDER_PRICE_HOME_BEST &&
        time_condition == BT_HUAXIN_TIME_GFD &&
        volume_condition == BT_HUAXIN_VOLUME_ANY &&
        (is_sse || is_szse)) {
        return {
            TORA_TSTP_OPT_HomeBestPrice,
            TORA_TSTP_TC_GFD,
            TORA_TSTP_VC_AV,
            true,
        };
    }
    if (order_price_type == BT_HUAXIN_ORDER_PRICE_OPPONENT_BEST &&
        time_condition == BT_HUAXIN_TIME_GFD &&
        volume_condition == BT_HUAXIN_VOLUME_ANY &&
        (is_sse || is_szse)) {
        return {
            TORA_TSTP_OPT_BestPrice,
            TORA_TSTP_TC_GFD,
            TORA_TSTP_VC_AV,
            true,
        };
    }
    if (order_price_type == BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL &&
        time_condition == BT_HUAXIN_TIME_IOC &&
        volume_condition == BT_HUAXIN_VOLUME_ANY &&
        (is_sse || is_szse)) {
        return {
            TORA_TSTP_OPT_FiveLevelPrice,
            TORA_TSTP_TC_IOC,
            TORA_TSTP_VC_AV,
            true,
        };
    }
    if (is_sse && order_price_type == BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL &&
        time_condition == BT_HUAXIN_TIME_GFD &&
        volume_condition == BT_HUAXIN_VOLUME_ANY) {
        return {
            TORA_TSTP_OPT_FiveLevelPrice,
            TORA_TSTP_TC_GFD,
            TORA_TSTP_VC_AV,
            true,
        };
    }
    if (is_szse && order_price_type == BT_HUAXIN_ORDER_PRICE_ANY &&
        time_condition == BT_HUAXIN_TIME_IOC &&
        volume_condition == BT_HUAXIN_VOLUME_ANY) {
        return {
            TORA_TSTP_OPT_AnyPrice,
            TORA_TSTP_TC_IOC,
            TORA_TSTP_VC_AV,
            true,
        };
    }
    if (is_szse && order_price_type == BT_HUAXIN_ORDER_PRICE_ANY &&
        time_condition == BT_HUAXIN_TIME_IOC &&
        volume_condition == BT_HUAXIN_VOLUME_ALL) {
        return {
            TORA_TSTP_OPT_AnyPrice,
            TORA_TSTP_TC_IOC,
            TORA_TSTP_VC_CV,
            true,
        };
    }
    return {};
}

constexpr bool order_semantics_are_supported(
    TTORATstpExchangeIDType exchange,
    uint8_t order_price_type,
    uint8_t time_condition,
    uint8_t volume_condition
) noexcept {
    return map_order_semantics(
        exchange,
        order_price_type,
        time_condition,
        volume_condition
    ).supported;
}

static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SSE,
    BT_HUAXIN_ORDER_PRICE_HOME_BEST,
    BT_HUAXIN_TIME_GFD,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SSE,
    BT_HUAXIN_ORDER_PRICE_OPPONENT_BEST,
    BT_HUAXIN_TIME_GFD,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SSE,
    BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL,
    BT_HUAXIN_TIME_IOC,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SSE,
    BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL,
    BT_HUAXIN_TIME_GFD,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SZSE,
    BT_HUAXIN_ORDER_PRICE_HOME_BEST,
    BT_HUAXIN_TIME_GFD,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SZSE,
    BT_HUAXIN_ORDER_PRICE_OPPONENT_BEST,
    BT_HUAXIN_TIME_GFD,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SZSE,
    BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL,
    BT_HUAXIN_TIME_IOC,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SZSE,
    BT_HUAXIN_ORDER_PRICE_ANY,
    BT_HUAXIN_TIME_IOC,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(order_semantics_are_supported(
    TORA_TSTP_EXD_SZSE,
    BT_HUAXIN_ORDER_PRICE_ANY,
    BT_HUAXIN_TIME_IOC,
    BT_HUAXIN_VOLUME_ALL
));
static_assert(
    map_order_semantics(
        TORA_TSTP_EXD_SSE,
        BT_HUAXIN_ORDER_PRICE_HOME_BEST,
        BT_HUAXIN_TIME_GFD,
        BT_HUAXIN_VOLUME_ANY
    ).order_price_type == TORA_TSTP_OPT_HomeBestPrice
);
static_assert(
    map_order_semantics(
        TORA_TSTP_EXD_SSE,
        BT_HUAXIN_ORDER_PRICE_OPPONENT_BEST,
        BT_HUAXIN_TIME_GFD,
        BT_HUAXIN_VOLUME_ANY
    ).order_price_type == TORA_TSTP_OPT_BestPrice
);
static_assert(
    map_order_semantics(
        TORA_TSTP_EXD_SSE,
        BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL,
        BT_HUAXIN_TIME_GFD,
        BT_HUAXIN_VOLUME_ANY
    ).time_condition == TORA_TSTP_TC_GFD
);
static_assert(
    map_order_semantics(
        TORA_TSTP_EXD_SSE,
        BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL,
        BT_HUAXIN_TIME_IOC,
        BT_HUAXIN_VOLUME_ANY
    ).time_condition == TORA_TSTP_TC_IOC
);
static_assert(
    map_order_semantics(
        TORA_TSTP_EXD_SZSE,
        BT_HUAXIN_ORDER_PRICE_ANY,
        BT_HUAXIN_TIME_IOC,
        BT_HUAXIN_VOLUME_ANY
    ).volume_condition == TORA_TSTP_VC_AV
);
static_assert(
    map_order_semantics(
        TORA_TSTP_EXD_SZSE,
        BT_HUAXIN_ORDER_PRICE_ANY,
        BT_HUAXIN_TIME_IOC,
        BT_HUAXIN_VOLUME_ALL
    ).volume_condition == TORA_TSTP_VC_CV
);
static_assert(!order_semantics_are_supported(
    TORA_TSTP_EXD_SSE,
    BT_HUAXIN_ORDER_PRICE_ANY,
    BT_HUAXIN_TIME_IOC,
    BT_HUAXIN_VOLUME_ANY
));
static_assert(!order_semantics_are_supported(
    TORA_TSTP_EXD_SZSE,
    BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL,
    BT_HUAXIN_TIME_GFD,
    BT_HUAXIN_VOLUME_ANY
));

constexpr bool known_order_price_type(uint8_t value) noexcept {
    return value >= BT_HUAXIN_ORDER_PRICE_LIMIT &&
           value <= BT_HUAXIN_ORDER_PRICE_ANY;
}

constexpr bool known_time_condition(uint8_t value) noexcept {
    return value == BT_HUAXIN_TIME_GFD || value == BT_HUAXIN_TIME_IOC;
}

constexpr bool known_volume_condition(uint8_t value) noexcept {
    return value == BT_HUAXIN_VOLUME_ANY || value == BT_HUAXIN_VOLUME_ALL;
}

bool validate_limit_order(const bt_huaxin_limit_order_request &order) noexcept {
    return bytes_are_c_text(order.exchange, order.exchange_size, BT_HUAXIN_EXCHANGE_CAPACITY) &&
           bytes_are_c_text(order.investor_id, order.investor_id_size, BT_HUAXIN_INVESTOR_CAPACITY) &&
           bytes_are_c_text(order.business_unit_id, order.business_unit_id_size, BT_HUAXIN_BUSINESS_UNIT_CAPACITY) &&
           bytes_are_c_text(order.shareholder_id, order.shareholder_id_size, BT_HUAXIN_SHAREHOLDER_CAPACITY) &&
           bytes_are_c_text(order.security, order.security_size, BT_HUAXIN_SECURITY_CAPACITY) &&
           order.exchange_size != 0u && order.investor_id_size != 0u &&
           order.shareholder_id_size != 0u && order.security_size != 0u &&
           order.direction <= 1u && std::isfinite(order.limit_price) &&
           order.limit_price > 0.0 && valid_limit_order_amount(order.amount) &&
           order.order_ref > 0;
}

bool validate_order(const bt_huaxin_order_request &order) noexcept {
    return bytes_are_c_text(order.exchange, order.exchange_size, BT_HUAXIN_EXCHANGE_CAPACITY) &&
           bytes_are_c_text(order.investor_id, order.investor_id_size, BT_HUAXIN_INVESTOR_CAPACITY) &&
           bytes_are_c_text(order.business_unit_id, order.business_unit_id_size, BT_HUAXIN_BUSINESS_UNIT_CAPACITY) &&
           bytes_are_c_text(order.shareholder_id, order.shareholder_id_size, BT_HUAXIN_SHAREHOLDER_CAPACITY) &&
           bytes_are_c_text(order.security, order.security_size, BT_HUAXIN_SECURITY_CAPACITY) &&
           order.exchange_size != 0u && order.investor_id_size != 0u &&
           order.shareholder_id_size != 0u && order.security_size != 0u &&
           order.direction <= 1u && known_order_price_type(order.order_price_type) &&
           known_time_condition(order.time_condition) &&
           known_volume_condition(order.volume_condition) &&
           std::isfinite(order.limit_price) && order.limit_price >= 0.0 &&
           valid_limit_order_amount(order.amount) && order.order_ref > 0;
}

bool valid_order_protection_price(
    TTORATstpExchangeIDType exchange,
    const bt_huaxin_order_request &order
) noexcept {
    if (!std::isfinite(order.limit_price) || order.limit_price < 0.0) {
        return false;
    }
    if (order.order_price_type == BT_HUAXIN_ORDER_PRICE_LIMIT ||
        exchange == TORA_TSTP_EXD_SSE) {
        return order.limit_price > 0.0;
    }
    return true;
}

bool validate_cancel_order(const bt_huaxin_cancel_order_request &cancel) noexcept {
    if (!bytes_are_c_text(cancel.exchange, cancel.exchange_size, BT_HUAXIN_EXCHANGE_CAPACITY) ||
        !bytes_are_c_text(cancel.order_sys_id, cancel.order_sys_id_size, BT_HUAXIN_ORDER_SYS_ID_CAPACITY) ||
        cancel.exchange_size == 0u) {
        return false;
    }
    const bool has_sys_id = cancel.order_sys_id_size != 0u;
    const bool has_session_identity =
        cancel.front_id > 0 && cancel.session_id != 0 && cancel.order_ref > 0;
    const bool partial_session_identity =
        cancel.front_id != 0 || cancel.session_id != 0 || cancel.order_ref != 0;
    if (partial_session_identity && !has_session_identity) {
        return false;
    }
    return has_sys_id || has_session_identity;
}

}  // namespace

extern "C" {

uint32_t bt_huaxin_abi_version(void) {
    return BT_HUAXIN_ABI_VERSION;
}

const char *bt_huaxin_bridge_version(void) {
    return "bullet-trade-huaxin-tora-trader/2";
}

const char *bt_huaxin_vendor_schema_id(void) {
    return kVendorSchemaId;
}

const char *bt_huaxin_field_set_version(void) {
    return kFieldSetVersion;
}

const char *bt_huaxin_error_message(int32_t result) {
    switch (result) {
        case BT_HUAXIN_OK: return "ok";
        case BT_HUAXIN_INVALID_ARGUMENT: return "invalid argument";
        case BT_HUAXIN_ABI_INCOMPATIBLE: return "ABI incompatible";
        case BT_HUAXIN_STRUCT_SIZE_INCOMPATIBLE: return "struct size incompatible";
        case BT_HUAXIN_ALLOCATION_FAILED: return "allocation failed";
        case BT_HUAXIN_INTERNAL_ERROR: return "internal error";
        case BT_HUAXIN_SCHEMA_INCOMPATIBLE: return "vendor schema or field set incompatible";
        case BT_HUAXIN_BUFFER_OWNERSHIP_ERROR: return "buffer ownership error";
        case BT_HUAXIN_UNSUPPORTED_REQUEST: return "unsupported request";
        case BT_HUAXIN_QUEUE_FULL: return "queue full";
        case BT_HUAXIN_SESSION_NOT_STARTED: return "session not started";
        case BT_HUAXIN_NOT_LOGGED_IN: return "not logged in";
        case BT_HUAXIN_TRADING_DISABLED: return "trading disabled";
        case BT_HUAXIN_CANCEL_DISABLED: return "cancel disabled";
        case BT_HUAXIN_NOT_READY: return "runtime not ready";
        case BT_HUAXIN_VENDOR_ERROR: return "vendor API returned an error";
        case BT_HUAXIN_INVALID_STATE: return "invalid runtime state";
        default: return "unknown result";
    }
}

int32_t bt_huaxin_create(
    const bt_huaxin_create_options *options,
    bt_huaxin_handle **out_handle
) {
    try {
        if (options == nullptr || out_handle == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        *out_handle = nullptr;
        const int32_t header_result = validate_header(
            options->abi_version,
            options->struct_size,
            sizeof(*options)
        );
        if (header_result != BT_HUAXIN_OK) {
            return header_result;
        }
        if (!schema_identity_matches(options->schema)) {
            return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
        }
        if (options->reserved != 0u || options->queue_capacity < 2u ||
            options->queue_capacity > 1000000u) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        std::unique_ptr<bt_huaxin_handle> handle{
            new (std::nothrow) bt_huaxin_handle{}
        };
        if (handle == nullptr) {
            return BT_HUAXIN_ALLOCATION_FAILED;
        }
        handle->queue_capacity = options->queue_capacity;
        emit_state_locked(handle.get(), 0);
        *out_handle = handle.release();
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_destroy(bt_huaxin_handle *handle) {
    if (handle == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t stop_result = stop_session(handle);
    delete handle;
    return stop_result;
}

int32_t bt_huaxin_get_health(
    bt_huaxin_handle *handle,
    bt_huaxin_health *out_health
) {
    if (handle == nullptr || out_health == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        out_health->abi_version,
        out_health->struct_size,
        sizeof(*out_health)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    if (!schema_identity_matches(out_health->schema)) {
        return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
    }
    std::lock_guard<std::mutex> lock(handle->mutex);
    out_health->state = handle->state;
    out_health->queue_capacity = handle->queue_capacity;
    out_health->queue_size = static_cast<uint32_t>(handle->events.size());
    out_health->reserved = 0u;
    out_health->dropped_events = handle->dropped_events;
    set_schema_identity(&out_health->schema);
    return BT_HUAXIN_OK;
}

int32_t bt_huaxin_start_session(
    bt_huaxin_handle *handle,
    const bt_huaxin_session_config *config
) {
    try {
        if (handle == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        const int32_t validation_result = validate_session_config(config);
        if (validation_result != BT_HUAXIN_OK) {
            return validation_result;
        }
        {
            std::lock_guard<std::mutex> lock(handle->mutex);
            if (handle->api != nullptr || handle->spi != nullptr) {
                return BT_HUAXIN_INVALID_STATE;
            }
            handle->session.clear_sensitive();
            handle->session = copy_session_values(*config);
            handle->state = BT_HUAXIN_STATE_CONNECTING;
            handle->last_error_id = 0;
            handle->initial_query_mask = 0u;
        }
        CTORATstpTraderApi *api = CTORATstpTraderApi::CreateTstpTraderApi(
            handle->session.flow_path.c_str(),
            handle->session.encrypt,
            trade_comm_value(handle->session.trade_comm_mode),
            handle->session.interface_address.c_str(),
            false
        );
        if (api == nullptr) {
            std::lock_guard<std::mutex> lock(handle->mutex);
            handle->session.clear_sensitive();
            handle->state = BT_HUAXIN_STATE_FAULTED;
            emit_state_locked(handle, BT_HUAXIN_VENDOR_ERROR);
            return BT_HUAXIN_VENDOR_ERROR;
        }
        std::unique_ptr<TraderSpi> spi{new (std::nothrow) TraderSpi(handle)};
        if (spi == nullptr) {
            api->Release();
            std::lock_guard<std::mutex> lock(handle->mutex);
            handle->session.clear_sensitive();
            return BT_HUAXIN_ALLOCATION_FAILED;
        }
        {
            std::lock_guard<std::mutex> lock(handle->mutex);
            handle->api = api;
            handle->spi = spi.release();
            emit_state_locked(handle, 0);
        }
        api->RegisterSpi(handle->spi);
        api->RegisterFront(handle->session.trade_front.data());
        api->SubscribePrivateTopic(topic_value(handle->session.private_topic));
        if (handle->session.public_topic != BT_HUAXIN_TOPIC_DISABLED) {
            api->SubscribePublicTopic(topic_value(handle->session.public_topic));
        }
        api->Init();
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_stop_session(bt_huaxin_handle *handle) {
    if (handle == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    return stop_session(handle);
}

int32_t bt_huaxin_get_trader_health(
    bt_huaxin_handle *handle,
    bt_huaxin_trader_health *out_health
) {
    if (handle == nullptr || out_health == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    const int32_t header_result = validate_header(
        out_health->abi_version,
        out_health->struct_size,
        sizeof(*out_health)
    );
    if (header_result != BT_HUAXIN_OK) {
        return header_result;
    }
    if (!schema_identity_matches(out_health->schema)) {
        return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
    }
    std::lock_guard<std::mutex> lock(handle->mutex);
    out_health->state = handle->state;
    out_health->queue_capacity = handle->queue_capacity;
    out_health->queue_size = static_cast<uint32_t>(handle->events.size());
    out_health->reserved = 0u;
    out_health->dropped_events = handle->dropped_events;
    out_health->transport_connected = handle->transport_connected ? 1u : 0u;
    out_health->logged_in = handle->logged_in ? 1u : 0u;
    out_health->ready_for_queries = handle->ready_for_queries ? 1u : 0u;
    out_health->ready_for_new_orders = ready_for_new_orders_locked(handle) ? 1u : 0u;
    out_health->ready_for_cancel = ready_for_cancel_locked(handle) ? 1u : 0u;
    std::memset(out_health->reserved_flags, 0, sizeof(out_health->reserved_flags));
    out_health->session_epoch = handle->session_epoch;
    out_health->last_error_id = handle->last_error_id;
    out_health->reserved_tail = 0u;
    set_schema_identity(&out_health->schema);
    return BT_HUAXIN_OK;
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
        CTORATstpTraderApi *api = nullptr;
        {
            std::lock_guard<std::mutex> lock(handle->mutex);
            if (handle->api == nullptr) {
                return BT_HUAXIN_SESSION_NOT_STARTED;
            }
            if (!handle->logged_in || !handle->ready_for_queries) {
                return BT_HUAXIN_NOT_LOGGED_IN;
            }
            const bool is_place_request =
                request->request_type == BT_HUAXIN_REQUEST_PLACE_LIMIT ||
                request->request_type == BT_HUAXIN_REQUEST_PLACE_ORDER;
            if (is_place_request && !handle->session.enable_trading) {
                return BT_HUAXIN_TRADING_DISABLED;
            }
            if (is_place_request && !ready_for_new_orders_locked(handle)) {
                return BT_HUAXIN_NOT_READY;
            }
            if (request->request_type == BT_HUAXIN_REQUEST_CANCEL_ORDER &&
                !handle->session.enable_cancel) {
                return BT_HUAXIN_CANCEL_DISABLED;
            }
            api = handle->api;
        }
        const int request_id = static_cast<int>(request->request_id);
        int vendor_result = 0;
        switch (request->request_type) {
            case BT_HUAXIN_REQUEST_QUERY_SECURITY: {
                if (request->payload_size != sizeof(bt_huaxin_query_request)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                bt_huaxin_query_request value{};
                std::memcpy(&value, request->payload, sizeof(value));
                if (!bytes_are_c_text(value.exchange, value.exchange_size, BT_HUAXIN_EXCHANGE_CAPACITY) ||
                    !bytes_are_c_text(value.security, value.security_size, BT_HUAXIN_SECURITY_CAPACITY)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                CTORATstpQrySecurityField query{};
                if (!parse_exchange(value.exchange, value.exchange_size, &query.ExchangeID) ||
                    !copy_vendor_text(query.SecurityID, bytes_to_string(value.security, value.security_size))) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                {
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    handle->queries[request_id] = {request->request_type, 0u};
                }
                vendor_result = api->ReqQrySecurity(&query, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_QUERY_SHAREHOLDER_ACCOUNT: {
                if (request->payload_size != 0u) return BT_HUAXIN_INVALID_ARGUMENT;
                CTORATstpQryShareholderAccountField query{};
                (void)copy_vendor_text(query.InvestorID, handle->session.login_account);
                {
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    handle->queries[request_id] = {request->request_type, 0u};
                }
                vendor_result = api->ReqQryShareholderAccount(&query, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_QUERY_TRADING_ACCOUNT: {
                if (request->payload_size != 0u) return BT_HUAXIN_INVALID_ARGUMENT;
                CTORATstpQryTradingAccountField query{};
                (void)copy_vendor_text(query.InvestorID, handle->session.login_account);
                {
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    handle->queries[request_id] = {request->request_type, 0u};
                }
                vendor_result = api->ReqQryTradingAccount(&query, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_QUERY_POSITION: {
                if (request->payload_size != 0u) return BT_HUAXIN_INVALID_ARGUMENT;
                CTORATstpQryPositionField query{};
                (void)copy_vendor_text(query.InvestorID, handle->session.login_account);
                {
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    handle->queries[request_id] = {request->request_type, 0u};
                }
                vendor_result = api->ReqQryPosition(&query, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_QUERY_ORDER: {
                if (request->payload_size != 0u) return BT_HUAXIN_INVALID_ARGUMENT;
                CTORATstpQryOrderField query{};
                {
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    handle->queries[request_id] = {request->request_type, 0u};
                }
                vendor_result = api->ReqQryOrder(&query, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_QUERY_TRADE: {
                if (request->payload_size != 0u) return BT_HUAXIN_INVALID_ARGUMENT;
                CTORATstpQryTradeField query{};
                {
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    handle->queries[request_id] = {request->request_type, 0u};
                }
                vendor_result = api->ReqQryTrade(&query, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_PLACE_LIMIT: {
                if (request->payload_size != sizeof(bt_huaxin_limit_order_request)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                bt_huaxin_limit_order_request value{};
                std::memcpy(&value, request->payload, sizeof(value));
                if (!validate_limit_order(value)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                CTORATstpInputOrderField order{};
                if (!parse_exchange(value.exchange, value.exchange_size, &order.ExchangeID) ||
                    !copy_vendor_text(order.InvestorID, bytes_to_string(value.investor_id, value.investor_id_size)) ||
                    !copy_vendor_text(order.BusinessUnitID, bytes_to_string(value.business_unit_id, value.business_unit_id_size)) ||
                    !copy_vendor_text(order.ShareholderID, bytes_to_string(value.shareholder_id, value.shareholder_id_size)) ||
                    !copy_vendor_text(order.SecurityID, bytes_to_string(value.security, value.security_size))) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                order.UserRequestID = request_id;
                order.Direction = value.direction == 0u ? TORA_TSTP_D_Buy : TORA_TSTP_D_Sell;
                order.LimitPrice = value.limit_price;
                order.VolumeTotalOriginal = static_cast<TTORATstpVolumeType>(value.amount);
                order.OrderPriceType = TORA_TSTP_OPT_LimitPrice;
                order.TimeCondition = TORA_TSTP_TC_GFD;
                order.VolumeCondition = TORA_TSTP_VC_AV;
                order.Operway = TORA_TSTP_OPERW_PCClient;
                order.OrderRef = value.order_ref;
                order.LotType = TORA_TSTP_LT_RoundLot;
                order.CondCheck = TORA_TSTP_CCT_None;
                order.ForceCloseReason = TORA_TSTP_FCC_NotForceClose;
                vendor_result = api->ReqOrderInsert(&order, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_PLACE_ORDER: {
                if (request->payload_size != sizeof(bt_huaxin_order_request)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                bt_huaxin_order_request value{};
                std::memcpy(&value, request->payload, sizeof(value));
                if (!validate_order(value)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                CTORATstpInputOrderField order{};
                if (!parse_exchange(value.exchange, value.exchange_size, &order.ExchangeID) ||
                    !copy_vendor_text(order.InvestorID, bytes_to_string(value.investor_id, value.investor_id_size)) ||
                    !copy_vendor_text(order.BusinessUnitID, bytes_to_string(value.business_unit_id, value.business_unit_id_size)) ||
                    !copy_vendor_text(order.ShareholderID, bytes_to_string(value.shareholder_id, value.shareholder_id_size)) ||
                    !copy_vendor_text(order.SecurityID, bytes_to_string(value.security, value.security_size))) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                const VendorOrderSemantics semantics = map_order_semantics(
                    order.ExchangeID,
                    value.order_price_type,
                    value.time_condition,
                    value.volume_condition
                );
                if (!semantics.supported) {
                    return BT_HUAXIN_UNSUPPORTED_REQUEST;
                }
                if (!valid_order_protection_price(order.ExchangeID, value)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                order.UserRequestID = request_id;
                order.Direction = value.direction == 0u ? TORA_TSTP_D_Buy : TORA_TSTP_D_Sell;
                order.LimitPrice = value.limit_price;
                order.VolumeTotalOriginal = static_cast<TTORATstpVolumeType>(value.amount);
                order.OrderPriceType = semantics.order_price_type;
                order.TimeCondition = semantics.time_condition;
                order.VolumeCondition = semantics.volume_condition;
                order.Operway = TORA_TSTP_OPERW_PCClient;
                order.OrderRef = value.order_ref;
                order.LotType = TORA_TSTP_LT_RoundLot;
                order.CondCheck = TORA_TSTP_CCT_None;
                order.ForceCloseReason = TORA_TSTP_FCC_NotForceClose;
                vendor_result = api->ReqOrderInsert(&order, request_id);
                break;
            }
            case BT_HUAXIN_REQUEST_CANCEL_ORDER: {
                if (request->payload_size != sizeof(bt_huaxin_cancel_order_request)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                bt_huaxin_cancel_order_request value{};
                std::memcpy(&value, request->payload, sizeof(value));
                if (!validate_cancel_order(value)) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                CTORATstpInputOrderActionField action{};
                if (!parse_exchange(value.exchange, value.exchange_size, &action.ExchangeID) ||
                    !copy_vendor_text(action.OrderSysID, bytes_to_string(value.order_sys_id, value.order_sys_id_size))) {
                    return BT_HUAXIN_INVALID_ARGUMENT;
                }
                action.UserRequestID = request_id;
                action.FrontID = value.front_id;
                action.SessionID = value.session_id;
                action.OrderRef = value.order_ref;
                action.ActionFlag = TORA_TSTP_AF_Delete;
                action.OrderActionRef = request_id;
                action.Operway = TORA_TSTP_OPERW_PCClient;
                vendor_result = api->ReqOrderAction(&action, request_id);
                break;
            }
            default:
                return BT_HUAXIN_UNSUPPORTED_REQUEST;
        }
        if (vendor_result != 0) {
            {
                std::lock_guard<std::mutex> lock(handle->mutex);
                handle->queries.erase(request_id);
            }
            emit_vendor_call_error(handle, request->request_id, request_id, vendor_result);
            return BT_HUAXIN_VENDOR_ERROR;
        }
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_drain_event_batch(
    bt_huaxin_handle *handle,
    uint32_t max_events,
    bt_huaxin_event_batch *out_batch
) {
    if (handle == nullptr || max_events == 0u || out_batch == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    return BT_HUAXIN_UNSUPPORTED_REQUEST;
}

int32_t bt_huaxin_free_event_batch(bt_huaxin_event_batch *batch) {
    if (batch == nullptr) {
        return BT_HUAXIN_INVALID_ARGUMENT;
    }
    return BT_HUAXIN_UNSUPPORTED_REQUEST;
}

int32_t bt_huaxin_drain_owned_event_batch(
    bt_huaxin_handle *handle,
    uint32_t max_events,
    bt_huaxin_owned_event_batch *out_batch
) {
    try {
        if (handle == nullptr || max_events == 0u || out_batch == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        const int32_t header_result = validate_header(
            out_batch->abi_version,
            out_batch->struct_size,
            sizeof(*out_batch)
        );
        if (header_result != BT_HUAXIN_OK) {
            return header_result;
        }
        if (!schema_identity_matches(out_batch->schema)) {
            return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
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
        out_batch->event_stride = static_cast<uint32_t>(sizeof(bt_huaxin_owned_event));
        set_schema_identity(&out_batch->schema);
        if (count == 0u) {
            return BT_HUAXIN_OK;
        }
        auto *events = static_cast<bt_huaxin_owned_event *>(
            std::calloc(static_cast<size_t>(count), sizeof(bt_huaxin_owned_event))
        );
        if (events == nullptr) {
            out_batch->event_stride = 0u;
            return BT_HUAXIN_ALLOCATION_FAILED;
        }
        uint64_t token = 0u;
        if (!register_batch_allocation(events, count, &token)) {
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
        out_batch->ownership_token = token;
        return BT_HUAXIN_OK;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

int32_t bt_huaxin_free_owned_event_batch(bt_huaxin_owned_event_batch *batch) {
    try {
        if (batch == nullptr) {
            return BT_HUAXIN_INVALID_ARGUMENT;
        }
        const int32_t header_result = validate_header(
            batch->abi_version,
            batch->struct_size,
            sizeof(*batch)
        );
        if (header_result != BT_HUAXIN_OK) {
            return header_result;
        }
        const bool schema_matches = schema_identity_matches(batch->schema);
        if (batch->ownership_token == 0u) {
            const bool empty = batch->events == nullptr && batch->event_count == 0u &&
                (batch->event_stride == 0u ||
                 batch->event_stride == sizeof(bt_huaxin_owned_event));
            batch->event_count = 0u;
            batch->event_stride = 0u;
            set_schema_identity(&batch->schema);
            if (!schema_matches) {
                return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
            }
            return empty ? BT_HUAXIN_OK : BT_HUAXIN_BUFFER_OWNERSHIP_ERROR;
        }
        BatchAllocation allocation{};
        if (!claim_batch_allocation(batch->ownership_token, &allocation)) {
            return BT_HUAXIN_BUFFER_OWNERSHIP_ERROR;
        }
        const bool metadata_matches = batch->events == allocation.events &&
            batch->event_count == allocation.event_count &&
            batch->event_stride == sizeof(bt_huaxin_owned_event);
        std::free(allocation.events);
        batch->events = nullptr;
        batch->ownership_token = 0u;
        batch->event_count = 0u;
        batch->event_stride = 0u;
        set_schema_identity(&batch->schema);
        if (!schema_matches) {
            return BT_HUAXIN_SCHEMA_INCOMPATIBLE;
        }
        return metadata_matches ? BT_HUAXIN_OK : BT_HUAXIN_BUFFER_OWNERSHIP_ERROR;
    } catch (...) {
        return BT_HUAXIN_INTERNAL_ERROR;
    }
}

}  // extern "C"
