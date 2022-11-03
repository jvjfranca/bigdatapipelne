from constructs import Construct
import builtins
from aws_cdk import aws_ecs, aws_iam


class Generator(Construct):

    def __init__(self, scope: "Construct", id: builtins.str, tps: int) -> None:

        super().__init__(scope, id)

        taskrole = aws_iam.Role(
            self,
            'taskrole',
            assumed_by=aws_iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            managed_policies=aws_iam.ManagedPolicy.from_aws_managed_policy_name('arn:aws:iam::aws:policy/AmazonKinesisFullAccess')
        )

        cluster = aws_ecs.Cluster(self, 'Cluster')

        taskdef = aws_ecs.FargateTaskDefinition(self, 'task-generator', task_role=taskrole)

        taskdef.add_container(
            'Generator',
            image = aws_ecs.ContainerImage.from_asset('./ddk_app/generator')
        )
        aws_ecs.FargateService(
            self,
            'GeneratorService',
            cluster=cluster,
            assign_public_ip=True,
            task_definition=taskdef,
            desired_count=tps
        )
