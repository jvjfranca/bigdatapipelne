from aws_cdk import Stack
import constructs
import builtins
import typing
from . import generator
from cdk_watchful import Watchful

class GeneratorStack(Stack):
    def __init__(self, scope: typing.Optional[constructs.Construct] = None, id: typing.Optional[builtins.str] = None) -> None:
        super().__init__(scope, id)

        generator.Generator(self,'GeneratorService',tps=1)

        wf = Watchful(self, 'watch-ecs')
        wf.watch_scope(self)