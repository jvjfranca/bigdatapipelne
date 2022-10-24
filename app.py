
import aws_cdk as cdk
from aws_ddk_core.cicd import CICDPipelineStack
from ddk_app.ddk_app_stack import DdkApplicationStack
from aws_ddk_core.config import Config


app = cdk.App()


class ApplicationStage(cdk.Stage):
    def __init__(
        self,
        scope,
        environment_id: str,
        **kwargs,
    ) -> None:
        super().__init__(
            scope, f"Ddk{environment_id.title()}Application", **kwargs)
        DdkApplicationStack(self, "DataPipeline", environment_id)


config = Config()
(
    CICDPipelineStack(
        app,
        id="DdkCodePipeline",
        environment_id="cicd",
        pipeline_name="ddk-application-pipeline",
    )
    .add_source_action(repository_name="ddk-repository")
    .add_synth_action()
    .build()
    .add_stage("dev", ApplicationStage(app, "dev", env=config.get_env("dev")))
    .add_stage("hom", ApplicationStage(app, "hom", env=config.get_env("hom")), manual_approvals=True)
    .add_stage("prod", ApplicationStage(app, "prod", env=config.get_env("prod")), manual_approvals=True)
    .synth()
)

app.synth()
