#ifndef BULLET_TRADE_HUAXIN_BRIDGE_H
#define BULLET_TRADE_HUAXIN_BRIDGE_H

/*
 * 作者: BruceLee
 * 文件职责: 定义不暴露厂商类型的华鑫 flat C ABI v2 离线与 Trader 合同。
 * 主要输入: 版本化 POD 会话/请求结构、opaque handle 和调用方初始化的输出描述符。
 * 主要输出: 稳定错误码、schema 身份、Trader health 与 bridge-owned 批量事件缓冲区。
 * 上下游关系: Python ctypes wrapper 调用；fake bridge 与 Trader-only bridge 实现本合同。
 * 关键约定: 公开头不包含 TORA/C++/STL 类型或厂商指针；凭据只在 start_session 期间深拷贝。
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
#define BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY 1024u
#define BT_HUAXIN_FLOW_PATH_CAPACITY 256u
#define BT_HUAXIN_FRONT_CAPACITY 256u
#define BT_HUAXIN_LOGIN_ACCOUNT_CAPACITY 32u
#define BT_HUAXIN_DEPARTMENT_CAPACITY 16u
#define BT_HUAXIN_PASSWORD_CAPACITY 64u
#define BT_HUAXIN_USER_PRODUCT_INFO_CAPACITY 10u
#define BT_HUAXIN_INTERFACE_PRODUCT_INFO_CAPACITY 32u
#define BT_HUAXIN_TERMINAL_INFO_CAPACITY 255u
#define BT_HUAXIN_MAC_ADDRESS_CAPACITY 20u
#define BT_HUAXIN_INTERFACE_ADDRESS_CAPACITY 128u
#define BT_HUAXIN_EXCHANGE_CAPACITY 8u
#define BT_HUAXIN_INVESTOR_CAPACITY 32u
#define BT_HUAXIN_BUSINESS_UNIT_CAPACITY 32u
#define BT_HUAXIN_SHAREHOLDER_CAPACITY 16u
#define BT_HUAXIN_SECURITY_CAPACITY 32u
#define BT_HUAXIN_SECURITY_NAME_CAPACITY 96u
#define BT_HUAXIN_ORDER_LOCAL_ID_CAPACITY 16u
#define BT_HUAXIN_ORDER_SYS_ID_CAPACITY 32u
#define BT_HUAXIN_TRADE_ID_CAPACITY 32u
#define BT_HUAXIN_DATE_CAPACITY 16u
#define BT_HUAXIN_TIME_CAPACITY 16u
#define BT_HUAXIN_ERROR_MESSAGE_CAPACITY 256u
#define BT_HUAXIN_NODE_INFO_CAPACITY 32u

/* session_config.reserved_flags 的已发布语义位；其余位必须保持为零。 */
#define BT_HUAXIN_SESSION_FLAG_ENABLE_NODE_TRANSFER 0x01u

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
    BT_HUAXIN_QUEUE_FULL = -9,
    BT_HUAXIN_SESSION_NOT_STARTED = -10,
    BT_HUAXIN_NOT_LOGGED_IN = -11,
    BT_HUAXIN_TRADING_DISABLED = -12,
    BT_HUAXIN_CANCEL_DISABLED = -13,
    BT_HUAXIN_NOT_READY = -14,
    BT_HUAXIN_VENDOR_ERROR = -15,
    BT_HUAXIN_INVALID_STATE = -16
} bt_huaxin_result;

typedef enum bt_huaxin_state {
    BT_HUAXIN_STATE_OFFLINE_READY = 1,
    BT_HUAXIN_STATE_CREATED = 10,
    BT_HUAXIN_STATE_CONNECTING = 11,
    BT_HUAXIN_STATE_FRONT_CONNECTED = 12,
    BT_HUAXIN_STATE_LOGIN_PENDING = 13,
    BT_HUAXIN_STATE_LOGGED_IN = 14,
    BT_HUAXIN_STATE_READY_READ_ONLY = 15,
    BT_HUAXIN_STATE_DISCONNECTED = 16,
    BT_HUAXIN_STATE_FAULTED = 17,
    BT_HUAXIN_STATE_CLOSED = 18
} bt_huaxin_state;

typedef enum bt_huaxin_request_type {
    /* 只读 fake 请求，用于验证 POD、二进制 payload 和 request_id 合同。 */
    BT_HUAXIN_REQUEST_PING = 1,
    BT_HUAXIN_REQUEST_QUERY_SECURITY = 100,
    BT_HUAXIN_REQUEST_QUERY_SHAREHOLDER_ACCOUNT = 101,
    BT_HUAXIN_REQUEST_QUERY_TRADING_ACCOUNT = 102,
    BT_HUAXIN_REQUEST_QUERY_POSITION = 103,
    BT_HUAXIN_REQUEST_QUERY_ORDER = 104,
    BT_HUAXIN_REQUEST_QUERY_TRADE = 105,
    BT_HUAXIN_REQUEST_QUERY_SYSTEM_NODE = 106,
    BT_HUAXIN_REQUEST_QUERY_FUND_TRANSFER_DETAIL = 107,
    BT_HUAXIN_REQUEST_QUERY_POSITION_TRANSFER_DETAIL = 108,
    BT_HUAXIN_REQUEST_PLACE_LIMIT = 120,
    BT_HUAXIN_REQUEST_CANCEL_ORDER = 121,
    BT_HUAXIN_REQUEST_PLACE_ORDER = 122,
    BT_HUAXIN_REQUEST_TRANSFER_FUND = 123,
    BT_HUAXIN_REQUEST_TRANSFER_POSITION = 124
} bt_huaxin_request_type;

typedef enum bt_huaxin_transfer_direction {
    BT_HUAXIN_TRANSFER_DIRECTION_ANY = 0,
    BT_HUAXIN_TRANSFER_NODE_MOVE_IN = 1,
    BT_HUAXIN_TRANSFER_NODE_MOVE_OUT = 2
} bt_huaxin_transfer_direction;

typedef enum bt_huaxin_transfer_position_type {
    BT_HUAXIN_TRANSFER_POSITION_ALL = 1,
    BT_HUAXIN_TRANSFER_POSITION_HISTORY = 2,
    BT_HUAXIN_TRANSFER_POSITION_TODAY_BUY_SELL = 3,
    BT_HUAXIN_TRANSFER_POSITION_TODAY_PURCHASE_REDEEM = 4,
    BT_HUAXIN_TRANSFER_POSITION_TODAY_SPLIT_MERGE = 5
} bt_huaxin_transfer_position_type;

typedef enum bt_huaxin_transfer_status {
    BT_HUAXIN_TRANSFER_STATUS_UNKNOWN = 0,
    BT_HUAXIN_TRANSFER_STATUS_HANDLING = 1,
    BT_HUAXIN_TRANSFER_STATUS_SUCCESS = 2,
    BT_HUAXIN_TRANSFER_STATUS_FAILED = 3,
    BT_HUAXIN_TRANSFER_STATUS_REPEAL_HANDLING = 4,
    BT_HUAXIN_TRANSFER_STATUS_REPEAL_SUCCESS = 5,
    BT_HUAXIN_TRANSFER_STATUS_REPEAL_FAILED = 6,
    BT_HUAXIN_TRANSFER_STATUS_EXTERNAL_ACCEPTED = 7,
    BT_HUAXIN_TRANSFER_STATUS_SENT_TO_ENGINE = 8
} bt_huaxin_transfer_status;

/*
 * 以下枚举是 BulletTrade 自有稳定值，不等于也不接受 TORA 原始 char 常量。
 * native bridge 只把受支持的交易所组合映射到厂商枚举。
 */
typedef enum bt_huaxin_order_price_type {
    BT_HUAXIN_ORDER_PRICE_LIMIT = 1,
    BT_HUAXIN_ORDER_PRICE_HOME_BEST = 2,
    BT_HUAXIN_ORDER_PRICE_OPPONENT_BEST = 3,
    BT_HUAXIN_ORDER_PRICE_FIVE_LEVEL = 4,
    BT_HUAXIN_ORDER_PRICE_ANY = 5
} bt_huaxin_order_price_type;

typedef enum bt_huaxin_time_condition {
    BT_HUAXIN_TIME_GFD = 1,
    BT_HUAXIN_TIME_IOC = 2
} bt_huaxin_time_condition;

typedef enum bt_huaxin_volume_condition {
    BT_HUAXIN_VOLUME_ANY = 1,
    BT_HUAXIN_VOLUME_ALL = 2
} bt_huaxin_volume_condition;

typedef enum bt_huaxin_event_type {
    BT_HUAXIN_EVENT_BRIDGE_CREATED = 1,
    BT_HUAXIN_EVENT_OFFLINE_READY = 2,
    BT_HUAXIN_EVENT_REQUEST_COMPLETED = 3,
    BT_HUAXIN_EVENT_STATE = 100,
    BT_HUAXIN_EVENT_ERROR = 101,
    BT_HUAXIN_EVENT_LOGIN = 102,
    BT_HUAXIN_EVENT_SECURITY = 110,
    BT_HUAXIN_EVENT_SHAREHOLDER_ACCOUNT = 111,
    BT_HUAXIN_EVENT_TRADING_ACCOUNT = 112,
    BT_HUAXIN_EVENT_POSITION = 113,
    BT_HUAXIN_EVENT_ORDER = 114,
    BT_HUAXIN_EVENT_TRADE = 115,
    BT_HUAXIN_EVENT_QUERY_END = 116,
    BT_HUAXIN_EVENT_SYSTEM_NODE = 117,
    BT_HUAXIN_EVENT_FUND_TRANSFER_DETAIL = 118,
    BT_HUAXIN_EVENT_POSITION_TRANSFER_DETAIL = 119,
    BT_HUAXIN_EVENT_ORDER_INSERT_RESPONSE = 120,
    BT_HUAXIN_EVENT_ORDER_ACTION_RESPONSE = 121,
    BT_HUAXIN_EVENT_FUND_TRANSFER_RESPONSE = 122,
    BT_HUAXIN_EVENT_POSITION_TRANSFER_RESPONSE = 123,
    BT_HUAXIN_EVENT_FUND_TRANSFER = 124,
    BT_HUAXIN_EVENT_POSITION_TRANSFER = 125
} bt_huaxin_event_type;

typedef enum bt_huaxin_login_account_type {
    BT_HUAXIN_LOGIN_USER_ID = 0,
    BT_HUAXIN_LOGIN_ACCOUNT_ID = 1,
    BT_HUAXIN_LOGIN_SHA_STOCK = 2,
    BT_HUAXIN_LOGIN_SZA_STOCK = 3,
    BT_HUAXIN_LOGIN_SHB_STOCK = 4,
    BT_HUAXIN_LOGIN_SZB_STOCK = 5,
    BT_HUAXIN_LOGIN_THREE_NEW_BOARD_A = 6,
    BT_HUAXIN_LOGIN_THREE_NEW_BOARD_B = 7,
    BT_HUAXIN_LOGIN_HK_STOCK = 8,
    BT_HUAXIN_LOGIN_UNIFIED_USER_ID = 9,
    BT_HUAXIN_LOGIN_BJ_STOCK = 10
} bt_huaxin_login_account_type;

typedef enum bt_huaxin_trade_comm_mode {
    BT_HUAXIN_TRADE_COMM_TCP = 0,
    BT_HUAXIN_TRADE_COMM_TCP_DIRECT = 3
} bt_huaxin_trade_comm_mode;

typedef enum bt_huaxin_topic_mode {
    BT_HUAXIN_TOPIC_DISABLED = -1,
    BT_HUAXIN_TOPIC_RESTART = 0,
    BT_HUAXIN_TOPIC_RESUME = 1,
    BT_HUAXIN_TOPIC_QUICK = 2
} bt_huaxin_topic_mode;

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

/* caller-owned；凭据字段均为显式长度 + bytes，bridge 必须在调用内深拷贝。 */
typedef struct bt_huaxin_session_config {
    uint32_t abi_version;
    uint32_t struct_size;
    uint8_t encrypt;
    uint8_t enable_trading;
    uint8_t enable_cancel;
    uint8_t reserved_flags;
    int32_t login_account_type;
    int32_t trade_comm_mode;
    int32_t private_topic;
    int32_t public_topic;
    uint32_t flow_path_size;
    uint8_t flow_path[BT_HUAXIN_FLOW_PATH_CAPACITY];
    uint32_t trade_front_size;
    uint8_t trade_front[BT_HUAXIN_FRONT_CAPACITY];
    uint32_t login_account_size;
    uint8_t login_account[BT_HUAXIN_LOGIN_ACCOUNT_CAPACITY];
    uint32_t department_id_size;
    uint8_t department_id[BT_HUAXIN_DEPARTMENT_CAPACITY];
    uint32_t password_size;
    uint8_t password[BT_HUAXIN_PASSWORD_CAPACITY];
    uint32_t dynamic_password_size;
    uint8_t dynamic_password[BT_HUAXIN_PASSWORD_CAPACITY];
    uint32_t user_product_info_size;
    uint8_t user_product_info[BT_HUAXIN_USER_PRODUCT_INFO_CAPACITY];
    uint32_t interface_product_info_size;
    uint8_t interface_product_info[BT_HUAXIN_INTERFACE_PRODUCT_INFO_CAPACITY];
    uint32_t terminal_info_size;
    uint8_t terminal_info[BT_HUAXIN_TERMINAL_INFO_CAPACITY];
    uint32_t mac_address_size;
    uint8_t mac_address[BT_HUAXIN_MAC_ADDRESS_CAPACITY];
    uint32_t interface_address_size;
    uint8_t interface_address[BT_HUAXIN_INTERFACE_ADDRESS_CAPACITY];
    bt_huaxin_schema_identity schema;
} bt_huaxin_session_config;

/* caller-owned；与基础 health 分离，不改变已发布的 fake 结构布局。 */
typedef struct bt_huaxin_trader_health {
    uint32_t abi_version;
    uint32_t struct_size;
    int32_t state;
    uint32_t queue_capacity;
    uint32_t queue_size;
    uint32_t reserved;
    uint64_t dropped_events;
    uint8_t transport_connected;
    uint8_t logged_in;
    uint8_t ready_for_queries;
    uint8_t ready_for_new_orders;
    uint8_t ready_for_cancel;
    uint8_t reserved_flags[3];
    uint64_t session_epoch;
    int32_t last_error_id;
    uint32_t reserved_tail;
    bt_huaxin_schema_identity schema;
} bt_huaxin_trader_health;

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

typedef struct bt_huaxin_query_request {
    uint32_t exchange_size;
    uint32_t security_size;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
} bt_huaxin_query_request;

typedef struct bt_huaxin_limit_order_request {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t business_unit_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    uint8_t direction;
    uint8_t reserved[3];
    double limit_price;
    uint32_t amount;
    int32_t order_ref;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t business_unit_id[BT_HUAXIN_BUSINESS_UNIT_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
} bt_huaxin_limit_order_request;

/*
 * caller-owned；价格/时效/成交量条件使用上面的稳定枚举，不暴露厂商原始字符。
 * 与旧 limit POD 保持相同尺寸，但使用独立 request type，旧 reserved 字节不会被误读。
 */
typedef struct bt_huaxin_order_request {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t business_unit_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    uint8_t direction;
    uint8_t order_price_type;
    uint8_t time_condition;
    uint8_t volume_condition;
    double limit_price;
    uint32_t amount;
    int32_t order_ref;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t business_unit_id[BT_HUAXIN_BUSINESS_UNIT_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
} bt_huaxin_order_request;

typedef struct bt_huaxin_cancel_order_request {
    uint32_t exchange_size;
    uint32_t order_sys_id_size;
    int32_t front_id;
    int32_t session_id;
    int32_t order_ref;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t order_sys_id[BT_HUAXIN_ORDER_SYS_ID_CAPACITY];
} bt_huaxin_cancel_order_request;

typedef struct bt_huaxin_system_node_query_request {
    int32_t node_id;
} bt_huaxin_system_node_query_request;

typedef struct bt_huaxin_fund_transfer_detail_query_request {
    uint32_t department_id_size;
    uint32_t account_id_size;
    uint32_t investor_id_size;
    uint8_t currency;
    uint8_t transfer_direction;
    uint8_t reserved[2];
    uint8_t department_id[BT_HUAXIN_DEPARTMENT_CAPACITY];
    uint8_t account_id[BT_HUAXIN_LOGIN_ACCOUNT_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
} bt_huaxin_fund_transfer_detail_query_request;

typedef struct bt_huaxin_position_transfer_detail_query_request {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t business_unit_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    uint8_t transfer_direction;
    uint8_t reserved[3];
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t business_unit_id[BT_HUAXIN_BUSINESS_UNIT_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
} bt_huaxin_position_transfer_detail_query_request;

typedef struct bt_huaxin_transfer_fund_request {
    uint32_t department_id_size;
    uint32_t account_id_size;
    int32_t apply_serial;
    int32_t external_node_id;
    uint8_t currency;
    uint8_t transfer_direction;
    uint8_t reserved[6];
    double amount;
    uint8_t department_id[BT_HUAXIN_DEPARTMENT_CAPACITY];
    uint8_t account_id[BT_HUAXIN_LOGIN_ACCOUNT_CAPACITY];
} bt_huaxin_transfer_fund_request;

typedef struct bt_huaxin_transfer_position_request {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t business_unit_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    int32_t apply_serial;
    int32_t volume;
    int32_t market_id;
    int32_t external_node_id;
    uint8_t transfer_direction;
    uint8_t transfer_position_type;
    uint8_t reserved[2];
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t business_unit_id[BT_HUAXIN_BUSINESS_UNIT_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
} bt_huaxin_transfer_position_request;

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

/* Trader 事件使用独立大 payload，不改变旧 fake event 的 192-byte ABI。 */
typedef struct bt_huaxin_owned_event {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t event_type;
    uint32_t payload_size;
    uint64_t sequence;
    int64_t received_ns;
    uint64_t request_id;
    bt_huaxin_schema_identity schema;
    uint8_t payload[BT_HUAXIN_OWNED_EVENT_PAYLOAD_CAPACITY];
} bt_huaxin_owned_event;

typedef struct bt_huaxin_owned_event_batch {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t event_count;
    uint32_t event_stride;
    bt_huaxin_schema_identity schema;
    const bt_huaxin_owned_event *events;
    uint64_t ownership_token;
} bt_huaxin_owned_event_batch;

typedef struct bt_huaxin_state_event {
    int32_t state;
    int32_t reason;
    uint8_t transport_connected;
    uint8_t logged_in;
    uint8_t ready_for_queries;
    uint8_t ready_for_new_orders;
    uint8_t ready_for_cancel;
    uint8_t reserved[3];
    uint64_t session_epoch;
} bt_huaxin_state_event;

typedef struct bt_huaxin_error_event {
    int32_t error_id;
    int32_t vendor_request_id;
    uint32_t message_size;
    uint8_t message[BT_HUAXIN_ERROR_MESSAGE_CAPACITY];
} bt_huaxin_error_event;

typedef struct bt_huaxin_login_event {
    int32_t front_id;
    int32_t session_id;
    int32_t max_order_ref;
    uint32_t trading_day_size;
    uint32_t login_time_size;
    uint8_t trading_day[BT_HUAXIN_DATE_CAPACITY];
    uint8_t login_time[BT_HUAXIN_TIME_CAPACITY];
} bt_huaxin_login_event;

typedef struct bt_huaxin_security_event {
    uint32_t exchange_size;
    uint32_t security_size;
    uint32_t security_name_size;
    uint32_t short_name_size;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
    uint8_t security_name[BT_HUAXIN_SECURITY_NAME_CAPACITY];
    uint8_t short_name[BT_HUAXIN_SECURITY_NAME_CAPACITY];
    int32_t market_id;
    int32_t security_type;
    int32_t order_unit;
    int32_t limit_buy_unit;
    int32_t limit_sell_unit;
    int32_t min_limit_buy;
    int32_t max_limit_buy;
    int32_t min_limit_sell;
    int32_t max_limit_sell;
    int32_t market_buy_unit;
    int32_t market_sell_unit;
    int32_t min_market_buy;
    int32_t max_market_buy;
    int32_t min_market_sell;
    int32_t max_market_sell;
    int32_t volume_multiple;
    uint8_t has_price_limit;
    uint8_t day_trading;
    uint8_t reserved_flags[2];
    int64_t security_status;
    double price_tick;
    double pre_close_price;
    double upper_limit_price;
    double lower_limit_price;
} bt_huaxin_security_event;

typedef struct bt_huaxin_shareholder_event {
    uint32_t investor_id_size;
    uint32_t exchange_size;
    uint32_t shareholder_id_size;
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    int32_t market_id;
    int32_t shareholder_id_type;
    uint8_t main_flag;
    uint8_t reserved[3];
} bt_huaxin_shareholder_event;

typedef struct bt_huaxin_account_event {
    uint32_t department_id_size;
    uint32_t account_id_size;
    uint8_t department_id[BT_HUAXIN_DEPARTMENT_CAPACITY];
    uint8_t account_id[BT_HUAXIN_LOGIN_ACCOUNT_CAPACITY];
    int32_t currency;
    int32_t reserved;
    double available_cash;
    double transferable_cash;
    double frozen_cash;
} bt_huaxin_account_event;

typedef struct bt_huaxin_position_event {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    uint32_t trading_day_size;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
    uint8_t trading_day[BT_HUAXIN_DATE_CAPACITY];
    /* v4.1.8 Volume=int、Price/Money=double；余额和可用量必须原样复制，不可推导。 */
    int32_t current_position;
    int32_t available_position;
    int32_t history_position;
    int32_t history_frozen;
    int32_t today_bs;
    int32_t today_bs_frozen;
    int32_t today_pr;
    int32_t today_pr_frozen;
    double total_cost;
    int32_t today_sm;
    int32_t today_sm_frozen;
    int32_t pre_position;
    int32_t pre_frozen;
    int32_t repay_untrade_volume;
    int32_t repay_transfer_untrade_volume;
    int32_t collateral_buy_untrade_volume;
    int32_t credit_buy_untrade_volume;
    int32_t credit_sell_untrade_volume;
    double history_position_price;
    double open_position_cost;
    double collateral_buy_untrade_amount;
    double credit_buy_untrade_amount;
    double credit_sell_untrade_amount;
    uint32_t business_unit_id_size;
    int32_t market_id;
    uint8_t business_unit_id[BT_HUAXIN_BUSINESS_UNIT_CAPACITY];
} bt_huaxin_position_event;

typedef struct bt_huaxin_system_node_event {
    int32_t node_id;
    uint32_t node_info_size;
    uint8_t current;
    uint8_t reserved[3];
    uint8_t node_info[BT_HUAXIN_NODE_INFO_CAPACITY];
} bt_huaxin_system_node_event;

typedef struct bt_huaxin_transfer_response_event {
    int32_t error_id;
    int32_t apply_serial;
    uint32_t message_size;
    uint8_t message[BT_HUAXIN_ERROR_MESSAGE_CAPACITY];
} bt_huaxin_transfer_response_event;

typedef struct bt_huaxin_fund_transfer_event {
    uint32_t department_id_size;
    uint32_t account_id_size;
    uint32_t investor_id_size;
    uint32_t business_unit_id_size;
    uint32_t operate_date_size;
    uint32_t operate_time_size;
    uint32_t status_message_size;
    int32_t fund_serial;
    int32_t apply_serial;
    int32_t front_id;
    int32_t session_id;
    int32_t external_node_id;
    uint8_t currency;
    uint8_t transfer_direction;
    uint8_t transfer_status;
    uint8_t reserved;
    double amount;
    uint8_t department_id[BT_HUAXIN_DEPARTMENT_CAPACITY];
    uint8_t account_id[BT_HUAXIN_LOGIN_ACCOUNT_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t business_unit_id[BT_HUAXIN_BUSINESS_UNIT_CAPACITY];
    uint8_t operate_date[BT_HUAXIN_DATE_CAPACITY];
    uint8_t operate_time[BT_HUAXIN_TIME_CAPACITY];
    uint8_t status_message[BT_HUAXIN_ERROR_MESSAGE_CAPACITY];
} bt_huaxin_fund_transfer_event;

typedef struct bt_huaxin_position_transfer_event {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t business_unit_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    uint32_t trading_day_size;
    uint32_t operate_date_size;
    uint32_t operate_time_size;
    uint32_t status_message_size;
    int32_t position_serial;
    int32_t apply_serial;
    int32_t front_id;
    int32_t session_id;
    int32_t market_id;
    int32_t external_node_id;
    int32_t history_volume;
    int32_t today_bs_volume;
    int32_t today_pr_volume;
    int32_t today_sm_volume;
    uint8_t transfer_direction;
    uint8_t transfer_position_type;
    uint8_t transfer_status;
    uint8_t reserved;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t business_unit_id[BT_HUAXIN_BUSINESS_UNIT_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
    uint8_t trading_day[BT_HUAXIN_DATE_CAPACITY];
    uint8_t operate_date[BT_HUAXIN_DATE_CAPACITY];
    uint8_t operate_time[BT_HUAXIN_TIME_CAPACITY];
    uint8_t status_message[BT_HUAXIN_ERROR_MESSAGE_CAPACITY];
} bt_huaxin_position_transfer_event;

typedef struct bt_huaxin_order_event {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    uint32_t order_local_id_size;
    uint32_t order_sys_id_size;
    uint32_t trading_day_size;
    uint32_t insert_time_size;
    uint32_t status_message_size;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
    uint8_t direction;
    uint8_t order_price_type;
    uint8_t time_condition;
    uint8_t volume_condition;
    uint8_t order_status;
    uint8_t submit_status;
    uint8_t reserved[2];
    double limit_price;
    int32_t amount;
    int32_t filled;
    int32_t canceled;
    int32_t front_id;
    int32_t session_id;
    int32_t order_ref;
    uint8_t order_local_id[BT_HUAXIN_ORDER_LOCAL_ID_CAPACITY];
    uint8_t order_sys_id[BT_HUAXIN_ORDER_SYS_ID_CAPACITY];
    uint8_t trading_day[BT_HUAXIN_DATE_CAPACITY];
    uint8_t insert_time[BT_HUAXIN_TIME_CAPACITY];
    uint8_t status_message[BT_HUAXIN_ERROR_MESSAGE_CAPACITY];
} bt_huaxin_order_event;

typedef struct bt_huaxin_trade_event {
    uint32_t exchange_size;
    uint32_t investor_id_size;
    uint32_t shareholder_id_size;
    uint32_t security_size;
    uint32_t trade_id_size;
    uint32_t order_sys_id_size;
    uint32_t order_local_id_size;
    uint32_t trade_date_size;
    uint32_t trade_time_size;
    uint32_t trading_day_size;
    uint8_t exchange[BT_HUAXIN_EXCHANGE_CAPACITY];
    uint8_t investor_id[BT_HUAXIN_INVESTOR_CAPACITY];
    uint8_t shareholder_id[BT_HUAXIN_SHAREHOLDER_CAPACITY];
    uint8_t security[BT_HUAXIN_SECURITY_CAPACITY];
    uint8_t direction;
    uint8_t reserved[3];
    uint8_t trade_id[BT_HUAXIN_TRADE_ID_CAPACITY];
    uint8_t order_sys_id[BT_HUAXIN_ORDER_SYS_ID_CAPACITY];
    uint8_t order_local_id[BT_HUAXIN_ORDER_LOCAL_ID_CAPACITY];
    int32_t order_ref;
    double price;
    int32_t amount;
    uint8_t trade_date[BT_HUAXIN_DATE_CAPACITY];
    uint8_t trade_time[BT_HUAXIN_TIME_CAPACITY];
    uint8_t trading_day[BT_HUAXIN_DATE_CAPACITY];
} bt_huaxin_trade_event;

typedef struct bt_huaxin_query_end_event {
    uint32_t request_type;
    int32_t error_id;
    uint32_t record_count;
    uint32_t message_size;
    uint8_t message[BT_HUAXIN_ERROR_MESSAGE_CAPACITY];
} bt_huaxin_query_end_event;

typedef struct bt_huaxin_order_response_event {
    int32_t error_id;
    int32_t order_ref;
    uint32_t order_sys_id_size;
    uint32_t message_size;
    uint8_t order_sys_id[BT_HUAXIN_ORDER_SYS_ID_CAPACITY];
    uint8_t message[BT_HUAXIN_ERROR_MESSAGE_CAPACITY];
} bt_huaxin_order_response_event;

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

BT_HUAXIN_API int32_t bt_huaxin_start_session(
    bt_huaxin_handle *handle,
    const bt_huaxin_session_config *config
);

BT_HUAXIN_API int32_t bt_huaxin_stop_session(bt_huaxin_handle *handle);

BT_HUAXIN_API int32_t bt_huaxin_get_trader_health(
    bt_huaxin_handle *handle,
    bt_huaxin_trader_health *out_health
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

BT_HUAXIN_API int32_t bt_huaxin_drain_owned_event_batch(
    bt_huaxin_handle *handle,
    uint32_t max_events,
    bt_huaxin_owned_event_batch *out_batch
);

BT_HUAXIN_API int32_t bt_huaxin_free_owned_event_batch(
    bt_huaxin_owned_event_batch *batch
);

#ifdef __cplusplus
}
#endif

#endif
