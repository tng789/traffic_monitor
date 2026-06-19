import video_processor
from video_processor import load_models
from web_server import app
from rmq import Producer
import uvicorn

from app_log import setup_logging, get_logger

logger = get_logger(__name__)


def main():
    """Main function to start the application"""
    setup_logging()
    logger.info("正在启动 traffic_monitor 服务")

    try:
        video_processor.producer = Producer(amqp_url='amqp://admin:zhxk12345@192.168.1.142:5672/')
    except Exception:
        logger.exception("无法创建 RabbitMQ Producer 实例")
        raise SystemExit(1)

    if video_processor.producer is None:
        logger.error("Producer 实例为 None，服务无法启动")
        raise SystemExit(1)

    logger.info("RabbitMQ Producer 已就绪")

    logger.info("正在装载模型...")
    try:
        load_models()
    except Exception:
        logger.exception("模型装载失败")
        raise SystemExit(1)
    logger.info("模型装载完成")

    logger.info("FastAPI 服务启动: host=0.0.0.0, port=8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
