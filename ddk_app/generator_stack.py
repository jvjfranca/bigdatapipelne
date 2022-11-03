from aws_cdk import Stack
import constructs
import builtins
import typing
from generator import Generator

class GeneratorStack(Stack):
    def __init__(self, scope: typing.Optional[constructs.Construct] = None, id: typing.Optional[builtins.str] = None) -> None:
        super().__init__(scope, id)

        Generator(self,'GeneratorService',tps=1)
