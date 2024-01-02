from __future__ import annotations
from typing import Any, List, Optional
from multiprocessing import Pool, cpu_count
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class CommandResult:
    """Represents the result of a command execution.

    Attributes:
        output: The output of the command.
        succeeded: A boolean indicating whether the command was successful.
        error: The exception raised by the command if it failed.
        error_message: The error message if the command failed.
        metadata: Any additional data.
        results: List of sub-commands results if applicable.
    """

    output: Any = None
    succeeded: bool = True
    error: Any = None
    error_message: str = None
    metadata: Any = None
    results: List[CommandResult] = field(default_factory=list)


class Command(ABC):
    """An abstract class for any executable command.

    Provides a simple common interface for all classes which implement
    the `Command` pattern. It provides a `run` for synchronous execution and
    an `async_run` for asynchronous execution.
    """

    @abstractmethod
    def run(self, input_data: Optional[Any] = None) -> CommandResult:
        """Runs the command.

        Args:
            input_data: The input for the command. Defaults to None.

        Returns:
            CommandResult: The result of executing the command.
        """

    @abstractmethod
    async def async_run(
        self, input_data: Optional[Any] = None
    ) -> CommandResult:
        """Runs the command asynchronously.

        Args:
            input_data: The input for the command. Defaults to None.

        Returns:
            CommandResult: The result after executing the command.
        """


def run_command(
    command: Command, input_data: Optional[Any] = None
) -> CommandResult:
    """Runs a command synchronously and returns the result.

    Args:
        command: A Command object to be executed.
        input_data: The input for the command. Defaults to None.

    Returns:
        CommandResult: The result of executing the command.
    """
    try:
        return command.run(input_data=input_data)
    except Exception as ex:
        return CommandResult(
            output=None,
            succeeded=False,
            error=ex,
            error_message=str(ex),
        )


async def async_run_command(
    command: Command, input_data: Optional[Any] = None
) -> CommandResult:
    """Runs a command asynchronously and returns the result.

    Args:
        command: A Command object to be executed.
        input_data: The input for the command. Defaults to None.
    Returns:
        CommandResult: The result of executing the command.
    """
    try:
        return await command.async_run(input_data=input_data)
    except Exception as ex:
        return CommandResult(
            output=None,
            succeeded=False,
            error=ex,
            error_message=str(ex),
        )


class PipeCommand(Command):
    """This is a Macro Command which runs the commands in sequence, similar to
    `Pipe` in Unix-like operating systems.
    The output of each command is provided as input to the next
    command in sequence. If any command fails, the pipeline stops and returns
    the result with success set to False. An error is raised if no commands
    list is provided.

    Attributes:
        commands: A list of Commands to be executed in the pipeline.
    """

    def __init__(
        self,
        commands: List[Command],
        collect_results=True,
    ):
        if not commands:
            raise ValueError("Commands list cannot be None or empty")
        self.commands = commands
        self._pipeline_failed = False
        self._last_result = None
        self._collect_results = collect_results
        self._results = []

    def _evaluate_execution(self, result: CommandResult) -> bool:
        """Evaluates the result of a command and returns a boolean indicating
        whether the process should continue or not.

        Args:
            result: The result of the command.

        Returns:
            bool: A boolean indicating whether the pipeline has failed or not.
        """
        self._last_result = result

        if self._collect_results:
            self._results.append(result)

        if not result.succeeded:
            self._pipeline_failed = True
            return False

        return True

    def _create_final_result(self, result: CommandResult) -> CommandResult:
        """Returns the final result of the pipeline. It returns the details of
        the last command in the pipeline.

        Args:
            result: The result of the last command in the sequence.

        Returns:
            CommandResult: The final result of the pipeline.
        """
        last_error = result.error
        last_error_message = result.error_message
        succeeded = result.succeeded
        output = result.output

        return CommandResult(
            output=output,
            succeeded=succeeded,
            results=self._results,
            error=last_error,
            error_message=last_error_message,
        )

    def run(self, input_data: Optional[Any] = None) -> CommandResult:
        for command in self.commands:
            if self._last_result:
                input_data = self._last_result.output

            result = run_command(command, input_data=input_data)
            if not self._evaluate_execution(result):
                break

        return self._create_final_result(result)

    async def async_run(
        self, input_data: Optional[Any] = None
    ) -> CommandResult:
        for command in self.commands:
            if self._last_result:
                input_data = self._last_result.output

            result = await async_run_command(command, input_data=input_data)
            if not self._evaluate_execution(result):
                break

        return self._create_final_result(result)


class SequentialCommand(Command):
    """This is a Macro Command which runs the commands sequentially with an
    option to set the operator between the commands.

    Each command runs after the previous command has finished in the sequence.
    The `operator` attribute sets the operation between the commands. This
    attribute is similar to the `&&`, `||` and `;` operators in Unix-like
    operating systems.

    Attributes:
        commands: A list of Command objects to be executed in sequence.
        operator: The operator between the commands. Defaults to `&&`.
            - If the operator is `&&` (default), then the next command will
            run only if the previous command was successful.
            - If the operator is `||`, then the next command will run only if
            the previous command failed.
            - If the operator is None, it will act like the `;` operator,
            meaning the next command will run regardless of the outcome of the
            previous command.
        collect_outputs: A boolean indicating whether to collect the outputs
            of all commands. Defaults to False.
            If collect_outputs is True, it gathers outputs of all results.
            Otherwise, the output is the output of the last command.
    """

    def __init__(
        self,
        commands: List[Command],
        operator="&&",
        collect_results=True,
    ):
        if not commands:
            raise ValueError("Commands list cannot be None or empty")
        if operator not in ["&&", "||", None]:
            raise ValueError("Invalid operator")

        self.commands = commands
        self.operator = operator
        self._collect_results = collect_results
        self._results = []
        self._outputs = []

    def _evaluate_execution(self, result: CommandResult) -> bool:
        """Evaluates the result of a command and returns a boolean indicating
        whether the process should continue or not.

        Args:
            result: The result of the command.

        Returns:
            bool: A boolean indicating whether to continue or not.
        """
        if self._collect_results:
            self._results.append(result)
            self._outputs.append(result.output)

        if not result.succeeded and self.operator == "&&":
            return False

        if result.succeeded and self.operator == "||":
            return False

        return True

    def _create_final_result(self, result: CommandResult) -> CommandResult:
        """Returns the final result of the sequence.

        Args:
            result: The result of the last command in the sequence.

        Returns:
            CommandResult: The final result of the sequence.
        """
        last_error = result.error
        last_error_message = result.error_message
        succeeded = result.succeeded
        output = self._outputs

        if not self.operator:
            # For None(;) operator, the final result is always succeeded.
            succeeded = True

        return CommandResult(
            output=output,
            succeeded=succeeded,
            results=self._results,
            error=last_error,
            error_message=last_error_message,
        )

    def run(self, input_data: Optional[Any] = None) -> CommandResult:
        for command in self.commands:
            result = run_command(command, input_data=input_data)
            if not self._evaluate_execution(result):
                break

        return self._create_final_result(result)

    async def async_run(
        self, input_data: Optional[Any] = None
    ) -> CommandResult:
        for command in self.commands:
            result = await async_run_command(command, input_data=input_data)
            if not self._evaluate_execution(result):
                break

        return self._create_final_result(result)


class MultiProcessCommand(Command):
    """This is a Macro Command which runs the commands in parallel using
    multiprocessing.

    The commands are indepndent of each other and can be run in parallel. This
    class uses the `multiprocessing.Pool` to run the commands in parallel.

    This class always collects the outputs of all commands' execution.

    Attributes:
        commands: A list of Command objects to be executed in parallel.
        pool_size: The number of concurrent processes to run the commands.
            Defaults to the number of CPUs available on the system.
    """

    def __init__(
        self,
        commands: List[Command],
        pool_size: int = None,
    ):
        if commands is None:
            raise ValueError("Commands list cannot be None")

        self.commands = commands
        self._pool_size = pool_size or cpu_count()
        self._results = []

    def run(self, input_data: Optional[Any] = None) -> CommandResult:
        if input_data:
            raise ValueError("ParallelCommand does not support input data.")

        with Pool(self._pool_size) as pool:
            self._results = pool.map(run_command, self.commands)

        outputs = [result.output for result in self._results if result]

        return CommandResult(output=outputs, results=self._results)

    async def async_run(
        self, input_data: Optional[Any] = None
    ) -> CommandResult:
        raise TypeError("ParallelCommand does not support async run")
