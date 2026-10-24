"""Failure notifications via SNS (email / Slack-via-Lambda / PagerDuty subscribers)."""
from __future__ import annotations

import logging

from ingestion.config import Config

log = logging.getLogger(__name__)


def send_alert(cfg: Config, subject: str, message: str) -> None:
    if not cfg.sns_topic_arn:
        log.warning("SNS_TOPIC_ARN not set; alert only logged", extra={"ctx": {"subject": subject}})
        return
    import boto3

    boto3.client("sns", region_name=cfg.aws_region).publish(
        TopicArn=cfg.sns_topic_arn, Subject=subject[:100], Message=message
    )
