from dataclasses import dataclass
from typing import Optional

@dataclass
class BrRequestSmootherConfig:
    notification_email: str
    average_request_duration: int = 5  # seconds
    minutes_to_process: int = 5
    max_concurrent_executions: int = 30
    alarm_threshold: int = 5
    alarm_evaluation_periods: int = 1
    dashboard_name: Optional[str] = "bedrock-errors"
    queue_depth_threshold: int = 100