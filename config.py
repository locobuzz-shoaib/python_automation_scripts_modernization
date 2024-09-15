import asyncio

from insta_webhook_utils_modernization.S3StorageManager import S3StorageManager
from insta_webhook_utils_modernization.insta_api_builder import InstagramAPI
from locobuzz_python_configuration import create_configuration
from locobuzz_python_configuration.logger_config import setup_logger
from locobuzz_python_configuration.utils_functions import setup_google_chat_messenger


INSTAGRAM_CHANNEL_GROUP_ID = 3
INSTAGRAM_MESSAGE_CHANNEL_ID = 70
INSTAGRAM_COMMENT_CHANNEL_ID = 22


async def configure():
    # Declaring user agents and referrer globally in case of no data found
    configuration = create_configuration(file_path='appsettings.json',
                                         required_components=["kafka", "clickhouse"]).__dict__

    broker = configuration['_broker']
    read_topic = configuration['_read_topic']
    push_topic = configuration['_push_topic']
    environ = configuration['_environ']
    service_name = "INSTAGRAM_WEBHOOK_COMMENTS_MODERNIZATION"
    is_async_logger = configuration.get('_is_async_logger', False)

    clickhouse_host = configuration['_clickhouse_host']
    clickhouse_port = configuration['_clickhouse_port']
    clickhouse_username = configuration['_clickhouse_username']
    clickhouse_password = configuration['_clickhouse_password']

    if is_async_logger:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        logger = setup_logger(service_name, async_mode=is_async_logger, log_level_str=configuration['_log_level'])
        logger.info("Async logger configured")
    else:
        logger = setup_logger(service_name, async_mode=is_async_logger, log_level_str=configuration['_log_level'])

    log_enabled = configuration.get('_log_enabled', ["PRODUCTION"]).split(",")

    logger.info(f"Log enabled for environment: {log_enabled}")
    logger.info(f"IS ASYNC LOGGER: {is_async_logger}")

    is_async_gchat = configuration['_extra_properties'].get("is_async_gchat", False)
    logger.info(f"IS ASYNC GCHAT: {is_async_gchat}")
    g_chat_hook = configuration['_extra_properties'].get("g_chat_webhook")
    error_gchat_hook = configuration['_extra_properties'].get("error_g_chat_webhook")
    g_chat = setup_google_chat_messenger(service_name, g_chat_hook, error_gchat_hook, environ,
                                         is_async_gchat, log_enabled, logger)
    logger.info(f"{service_name} Setting initialized successfully")
    instagram_api = InstagramAPI(base_url="https://graph.facebook.com", api_version="v19.0")

    post_fields_attributes = configuration['_extra_properties'].get("post_fields_attributes", {})
    reply_comment_parent_fields_attributes = configuration['_extra_properties'].get(
        "reply_comment_parent_fields_attributes", {})
    return broker, read_topic, push_topic, environ, service_name, is_async_logger, logger, g_chat, instagram_api, clickhouse_host, clickhouse_port, clickhouse_password, clickhouse_username,  post_fields_attributes, reply_comment_parent_fields_attributes


BROKER, READ_TOPIC, PUSH_TOPIC, ENVIRON, SERVICE_NAME, IS_ASYNC_LOGGER, LOGGER, G_CHAT, INSTAGRAM_API, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_PASSWORD, CLICKHOUSE_USERNAME, POST_FIELDS_ATTRIBUTES, REPLY_PARENT_ATTRIBUTES = asyncio.run(
    configure())
