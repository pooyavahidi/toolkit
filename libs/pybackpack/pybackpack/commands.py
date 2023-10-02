from typing import Any, List, Optional
from multiprocessing import Pool, cpu_count
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class CommandResult:
    """Represents the result of a command execution.

    Attributes:
        output: The output of the command.
        succeeded: A boolean indicating whether the command was successful.
        error: The exception raised by the command if it failed.
        error_message: The error message if the command failed.
        metadata: A dictionary containing any additional metadata.
    """

    output: Any
    succeeded: bool = True
    error: Any = None
    error_message: str = None
    metadata: Any = None


class Command(ABC):
    """
    An abstract class for any executable command.

    Provides a simple common interface for all classes which implement
    the `Command` pattern. It provides a `run` for synchronous execution and
    an `async_run` for asynchronous execution.

    The `run` function doesn't raise any exceptions. Instead, it sets the
    `result` attribute of the command object with the result of the command
    and reutrns it. If the command fails, the `result` attribute is set with
    the error and the `succeeded` attribute is set to False.

    Attributes:
        input_data: The input provided for the command.
        result: The result after executing the command.
    """

    def __init__(self, input_data: Optional[Any] = None):
        self.input_data = input_data
        self.result: Optional[CommandResult] = None

    def _handle_error(self, error: Exception) -> CommandResult:
        """Handles the error raised by the command.

        Args:
            error: The exception raised by the command.

        Returns:
            CommandResult: The result after handling the error.
        """
        return CommandResult(
            output=None,
            succeeded=False,
            error=error,
            error_message=str(error),
        )

    def _set_input(self, input_data: Any) -> None:
        """Checks the input provided and if not None, replace the command
        input with the new input.

        Args:
            input_data: The new input data.
        """
        if input_data is not None:
            self.input_data = input_data

    def run(self, input_data: Optional[Any] = None) -> CommandResult:
        """Runs the command.

        If `input_data` is provided, it sets the command input,
        and then calls the _run method to execute.

        Args:
            input_data: The input for the command. Defaults to None.

        Returns:
            CommandResult: The result after executing the command.
        """
        self._set_input(input_data)

        try:
            self.result = self._run()
        except Exception as ex:
            self.result = self._handle_error(ex)

        return self.result

    @abstractmethod
    def _run(self) -> CommandResult:
        """Executes the command and returns a CommandResult."""
        raise NotImplementedError

    async def async_run(
        self, input_data: Optional[Any] = None
    ) -> CommandResult:
        """Runs the command asynchronously.

        If `input_data` is provided, it sets the command input,
        and then calls the _async_run method to execute.

        Args:
            input_data: The input for the command. Defaults to None.

        Returns:
            CommandResult: The result after executing the command.
        """
        self._set_input(input_data)

        try:
            self.result = await self._async_run()
        except Exception as ex:
            self.result = self._handle_error(ex)

        return self.result

    @abstractmethod
    async def _async_run(self) -> CommandResult:
        """Executes the command asynchronously and returns a CommandResult."""
        raise NotImplementedError


class PipeCommand(Command):
    """This is a Macro Command which runs the commands in sequence, similar to
    a shell's pipe. The output of each command is provided as input to the next
    command in sequence. If any command fails, the pipeline stops and returns
    the result with success set to False. An error is raised if no commands
    list is provided.

    Attributes:
        commands (List[Command]): A list of Command objects to be executed in
            a pipeline.
        input_data: Initial input for the first command. Defaults to None.
    """

    def __init__(
        self,
        commands: List[Command],
        input_data: Any = None,
    ):
        super().__init__(input_data=input_data)

        if not commands:
            raise ValueError("Commands list cannot be None or empty")
        self.commands = commands
        self._pipeline_failed = False
        self._last_result = None

    def _evaluate_result(self, result: CommandResult) -> bool:
        """Evaluates the result of a command and returns a boolean indicating
        whether the process should continue or not.

        Args:
            result: The result of the command.

        Returns:
            bool: A boolean indicating whether the pipeline has failed or not.
        """
        self._last_result = result

        if not result.succeeded:
            self._pipeline_failed = True
            return False

        return True

    def _final_result(self) -> CommandResult:
        """Returns the final result of the pipeline.

        If the pipeline has failed, it returns the result of the last command
        which failed as is. Otherwise, it creates a new CommandResult object
        with the output of the last command and returns it.

        Returns:
            CommandResult: The final result of the pipeline.
        """
        if self._pipeline_failed:
            return self._last_result

        return CommandResult(output=self._last_result.output)

    def _last_output(self) -> Any:
        """Returns the output of the last command in the pipeline.
        If no result yet, it returns the command's input to the first command
        in the pipeline.
        """
        if self._last_result:
            return self._last_result.output

        return self.input_data

    def _run(self) -> CommandResult:
        for command in self.commands:
            result = command.run(input_data=self._last_output())
            if not self._evaluate_result(result):
                break

        return self._final_result()

    async def _async_run(self) -> CommandResult:
        for command in self.commands:
            result = await command.async_run(input_data=self._last_output())
            if not self._evaluate_result(result):
                break

        return self._final_result()


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
        collect_outputs=False,
    ):
        super().__init__()

        if not commands:
            raise ValueError("Commands list cannot be None or empty")
        if operator not in ["&&", "||", None]:
            raise ValueError("Invalid operator")

        self.commands = commands
        self.operator = operator
        self.collect_outputs = collect_outputs
        self._outputs = []

    def _evaluate_result(self, result: CommandResult) -> bool:
        """Evaluates the result of a command and returns a boolean indicating
        whether the process should continue or not.

        Args:
            result: The result of the command.

        Returns:
            bool: A boolean indicating whether to continue or not.
        """
        if self.collect_outputs:
            self._outputs.append(result.output)

        if not result.succeeded and self.operator == "&&":
            return False

        if result.succeeded and self.operator == "||":
            return False

        return True

    def _final_result(self, result: CommandResult) -> CommandResult:
        """Returns the final result of the sequence.

        If operator is `&&` or `||`, the final result is the result of the last
        command. If the operator is None, the final result is successed if all
        commands in the sequence executed regardless of their result.

        Args:
            result: The result of the last command in the sequence.

        Returns:
            CommandResult: The final result of the sequence.
        """
        # If not collecting, the outputs is the output of the last command.
        if not self.collect_outputs:
            self._outputs = [result.output]

        if self.operator:
            succeeded = result.succeeded
        else:
            succeeded = True

        return CommandResult(output=self._outputs, succeeded=succeeded)

    def _run(self) -> CommandResult:
        for command in self.commands:
            result = command.run()
            if not self._evaluate_result(result):
                break

        return self._final_result(result)

    async def _async_run(self) -> CommandResult:
        for command in self.commands:
            result = await command.async_run()
            if not self._evaluate_result(result):
                break

        return self._final_result(result)


def _execute_command(command: Command) -> CommandResult:
    """Function to execute a given command.

    The main purpose of this function is to be used with the `multiprocessing`
    module to run commands in parallel. The `multiprocessing.Pool` requires
    that the function to be executed is picklable. In order to reduce the
    complexity of the `Command` classes, this function is used to execute the
    command instead of calling the `run` method directly.

    Args:
        command: A Command object to execute.

    Returns:
        CommandResult: The result after executing the command.
    """
    return command.run()


class ParallelCommand(Command):
    """This is a Macro Command which runs the commands in parallel using
    multiprocessing.

    The commands are indepndent of each other and can be run in parallel. This
    class uses the `multiprocessing.Pool` to run the commands in parallel.

    Attributes:
        commands: A list of Command objects to be executed in parallel.
        number_of_processes: The number of processes to be used to run the
            commands. Defaults to the number of CPUs available on the system.
        collect_outputs: A boolean indicating whether to collect the outputs
            of all commands. Defaults to True.
    """

    def __init__(
        self,
        commands: List[Command],
        number_of_processes: int = None,
        collect_outputs=False,
    ):
        super().__init__()
        if commands is None:
            raise ValueError("Commands list cannot be None")

        self.commands = commands
        self.collect_outputs = collect_outputs
        self.pool_size = number_of_processes or cpu_count()

    def _run(self) -> CommandResult:
        outputs = None

        with Pool(self.pool_size) as pool:
            results = pool.map(_execute_command, self.commands)

        # Update each command's result attribute with the returned results.
        # This is because using multiprocessing, each command is run in a
        # separate process. Each process has its own memory space. Thus, any
        # change to the command object inside the child process (like setting
        # the result attribute) will not be reflected in the command object of
        # the parent process.
        for i, command in enumerate(self.commands):
            command.result = results[i]

        # If collect_outputs is True, gather outputs of all results.
        if self.collect_outputs:
            outputs = [result.output for result in results if result]

        return CommandResult(output=outputs)

    async def _async_run(self) -> CommandResult:
        raise TypeError("ParallelCommand does not support async run")
