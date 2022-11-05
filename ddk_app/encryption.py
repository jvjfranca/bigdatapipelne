from constructs import Construct
import builtins

from aws_cdk import (
    aws_kms as kms,
    aws_iam as iam,
    RemovalPolicy,
)

class Encryption(Construct):

    def __init__(self, scope: "Construct", id: builtins.str, account, region) -> None:

        super().__init__(scope, id)

        self.account = account
        self.region = region

        self.kms_policy = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "kms:*",
                    ],
                    principals=[iam.AccountRootPrincipal()],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "kms:Encrypt*",
                        "kms:Decrypt*",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:Describe*"
                    ],
                    principals=[
                        iam.ServicePrincipal("logs.us-east-1.amazonaws.com"),
                        iam.ServicePrincipal("glue.amazonaws.com")
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    actions=[
                        "kms:Decrypt*",
                    ],
                    principals=[
                        iam.ArnPrincipal(f"arn:aws:iam::{self.account}:role/service-role/aws-quicksight-service-role-v0")
                    ],
                    resources=["*"]
                )
            ]
        )

        self.kms_cmk_key = kms.Key(
            self,
            "bbbankkey",
            policy=self.kms_policy,
            removal_policy=RemovalPolicy.DESTROY
        )

        self.kms_cmk_key.add_alias('cmk-bbbank')

    def key(self):
        return self.kms_cmk_key