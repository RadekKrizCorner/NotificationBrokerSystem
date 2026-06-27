class ProducerQuotaExceeded(Exception):
    def __init__(self, *, retry_after_seconds: int) -> None:
        super().__init__("producer request quota exceeded")
        self.retry_after_seconds = retry_after_seconds


class FanoutLimitExceeded(Exception):
    def __init__(self, *, recipients: int, deliveries: int) -> None:
        super().__init__("notification fanout limit exceeded")
        self.recipients = recipients
        self.deliveries = deliveries


class IdempotencyConflict(Exception):
    pass
