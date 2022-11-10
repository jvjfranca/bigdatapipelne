from typing import Any

from aws_ddk_core.base import BaseStack
from constructs import Construct
from .custom import(
    Encryption,
    HistoricalAnalytics,
    RealTimeAnalytics
)


class DdkApplicationStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)


        encryption_key = Encryption(
            self,
            'encryption-key',
            self.account,
            self.region
        )

        HistoricalAnalytics(
            self,
            'historical-analytics',
            environment_id=environment_id,
            kms_cmk_key=encryption_key.key(),
            region=self.region,
            account=self.account
        )

        RealTimeAnalytics(
            self,
            'realtime-analytics',
            environment_id=environment_id,
            kms_cmk_key=encryption_key.key(),
            region=self.region,
            account=self.account
        )