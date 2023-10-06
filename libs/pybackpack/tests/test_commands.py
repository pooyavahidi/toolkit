import os
import pytest
from pybackpack.commands import (
    Command,
    CommandResult,
    SequentialCommand,
    ParallelCommand,
    PipeCommand,
)

# pylint: disable=missing-class-docstring,too-few-public-methods


@pytest.fixture
def in_vscode_launch(request):
    """Return True if running in VSCode's debugger."""
    return request.config.getoption("--vscode-launch", default=False)


class AddCharCommand(Command):
    def __init__(self, char=None) -> None:
        super().__init__()
        self.char = char

    def _run(self):
        if not self.input_data:
            self.input_data = ""
        return CommandResult(f"{self.input_data}{self.char}")

    async def _async_run(self) -> CommandResult:
        return self._run()


class ErrorCommand(Command):
    def __init__(self, raise_error=False) -> None:
        super().__init__()
        self.error_message = "Error from ErrorCommand"
        self.raise_error = raise_error

    def _run(self):
        if self.raise_error:
            raise SystemError(self.error_message)

        return CommandResult(
            output=None,
            succeeded=False,
            error_message=self.error_message,
        )

    async def _async_run(self) -> CommandResult:
        return self._run()


class ProcessInfoCommand(Command):
    def __init__(self) -> None:
        super().__init__()

    def _run(self):
        return CommandResult(output=os.getpid())

    async def _async_run(self) -> CommandResult:
        return self._run()


def test_single_command():
    cmd = AddCharCommand("A")
    result = cmd.run()
    assert result.output == "A"
    assert result.succeeded is True

    # Command with error
    cmd = ErrorCommand(raise_error=True)
    res = cmd.run()
    assert res.succeeded is False
    assert res.output is None
    assert isinstance(cmd.result.error, SystemError)
    assert res.error_message == "Error from ErrorCommand"

    # Command with error which doesn't raise error
    cmd = ErrorCommand(raise_error=False)
    result = cmd.run()
    assert result.output is None
    assert result.succeeded is False
    assert result.error_message == "Error from ErrorCommand"

    # Test always raising error command
    cmd = ErrorCommand(raise_error=True)
    res = cmd.run()
    assert cmd.result.succeeded is False
    assert cmd.result.output is None
    assert isinstance(cmd.result.error, SystemError)

    # Pass None as input
    cmd = AddCharCommand("A")
    result = cmd.run(None)
    assert result.output == "A"
    assert result.succeeded is True

    # Pass empty string as input
    cmd = AddCharCommand("A")
    result = cmd.run("")
    assert result.output == "A"
    assert result.succeeded is True


@pytest.mark.asyncio
async def test_single_command_async():
    # Command without error
    cmd = AddCharCommand(char="A")
    result = await cmd.async_run()
    assert result.output == "A"
    assert result.succeeded is True

    # Command which raises error
    cmd = ErrorCommand(raise_error=True)
    res = await cmd.async_run()
    assert res.succeeded is False
    assert res.output is None
    assert isinstance(cmd.result.error, SystemError)
    assert res.error_message == "Error from ErrorCommand"

    # Command with error which doesn't raise error
    cmd = ErrorCommand(raise_error=False)
    result = await cmd.async_run()
    assert result.output is None
    assert result.succeeded is False
    assert result.error_message == "Error from ErrorCommand"


def test_pipe():
    # Test pipe with simple commands, no errors
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = pipe.run()
    assert result.output == "ABC"

    # Test pipe with initial input to the pipe
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = pipe.run("D")
    assert result.output == "DABC"
    assert pipe.commands[0].result.output == "DA"
    assert pipe.commands[0].result.succeeded is True
    assert pipe.commands[1].result.output == "DAB"
    assert pipe.commands[2].result.output == "DABC"

    # Test pipe with command with error which doesn't raise error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = pipe.run()
    assert result.output is None
    assert result.succeeded is False
    assert result.error_message == "Error from ErrorCommand"
    # The error is None as Command doesn't raise error
    assert result.error is None
    assert pipe.commands[0].result.output == "A"

    # Test pipe with command with error which raises error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=True),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = pipe.run()
    assert result.succeeded is False
    assert result.output is None
    assert isinstance(result.error, SystemError)

    # Test pipe with None commands
    with pytest.raises(ValueError):
        pipe = PipeCommand(commands=None)

    # Test pipe with empty commands
    with pytest.raises(ValueError):
        pipe = PipeCommand(commands=[])


@pytest.mark.asyncio
async def test_pipe_async():
    # Test pipe with simple commands, no errors
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = await pipe.async_run()
    assert result.succeeded is True
    assert result.output == "ABC"

    # Test pipe with initial input to the pipe
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = await pipe.async_run("D")
    assert result.output == "DABC"
    assert pipe.commands[0].result.output == "DA"
    assert pipe.commands[0].result.succeeded is True
    assert pipe.commands[1].result.output == "DAB"
    assert pipe.commands[2].result.output == "DABC"

    # Test pipe with command with error which doesn't raise error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = await pipe.async_run()
    assert result.output is None
    assert result.succeeded is False
    assert result.error_message == "Error from ErrorCommand"
    assert result.error is None
    assert pipe.commands[0].result.output == "A"

    # Test pipe with command with error which raises error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=True),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    result = await pipe.async_run()
    assert result.succeeded is False
    assert result.output is None
    assert isinstance(result.error, SystemError)


def test_sequential_and_operator():
    # Simulate && in unix-like systems
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, collect_outputs=True)
    result = seq.run()
    assert result.output == ["A", "B", "C"]
    assert result.succeeded is True

    # Simulate && with error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, collect_outputs=True)
    result = seq.run()
    assert result.output == ["A", None]
    assert result.succeeded is False

    # Simulate && with error, not collecting outputs
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands)
    result = seq.run()
    assert result.output == [None]
    assert result.succeeded is False

    # Test with no commands
    with pytest.raises(ValueError):
        seq = SequentialCommand(commands=None)

    # Test with empty commands
    with pytest.raises(ValueError):
        seq = SequentialCommand(commands=[])


def test_sequential_or_operator():
    # Simulate || in unix-like system
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, operator="||")
    result = seq.run()
    assert result.output == ["A"]

    # Simulate || with error early
    commands = [
        ErrorCommand(raise_error=False),
        ErrorCommand(raise_error=True),
        AddCharCommand("A"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, operator="||")
    result = seq.run()
    assert result.output == ["A"]
    assert result.succeeded is True
    assert seq.commands[0].result.output is None
    assert seq.commands[1].result.output is None
    assert seq.commands[2].result.output == "A"

    # Simulate || without error
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, operator="||")
    result = seq.run()
    assert result.output == ["A"]
    assert result.succeeded is True

    # Simulate || with collecting outputs
    commands = [
        ErrorCommand(raise_error=False),
        AddCharCommand("A"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, operator="||", collect_outputs=True)
    result = seq.run()
    assert result.output == [None, "A"]
    assert result.succeeded is True
    assert seq.commands[0].result.output is None
    assert seq.commands[1].result.output == "A"
    assert seq.commands[2].result is None


def test_sequential_no_operator():
    # Simulate ; in unix-like system
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
        ErrorCommand(raise_error=True),
        AddCharCommand("B"),
        ErrorCommand(raise_error=True),
    ]
    seq = SequentialCommand(commands, operator=None, collect_outputs=True)
    result = seq.run()
    assert [cmd.result.succeeded for cmd in seq.commands] == [
        True,
        False,
        True,
        False,
        True,
        False,
    ]
    assert result.output == ["A", None, "C", None, "B", None]
    assert seq.commands[1].result.succeeded is False
    assert seq.commands[1].result.error is None
    assert isinstance(seq.commands[3].result.error, SystemError)
    assert seq.commands[5].result.succeeded is False
    assert isinstance(seq.commands[5].result.error, SystemError)

    # Simulate ; with error when raise_error=True
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
        ErrorCommand(raise_error=True),
        AddCharCommand("B"),
        ErrorCommand(raise_error=True),
    ]
    seq = SequentialCommand(commands, operator=None, collect_outputs=True)
    result = seq.run()
    assert result.output == ["A", None, "C", None, "B", None]


def test_sequential_combined():
    cmd1 = SequentialCommand(
        [AddCharCommand("A"), AddCharCommand("B")], collect_outputs=True
    )
    cmd2 = SequentialCommand(
        [AddCharCommand("C"), AddCharCommand("D")], collect_outputs=True
    )
    seq = SequentialCommand([cmd1, cmd2], collect_outputs=True)
    result = seq.run()
    assert result.output == [["A", "B"], ["C", "D"]]
    assert result.succeeded is True
    assert seq.commands[0].result.output == ["A", "B"]
    assert seq.commands[1].commands[0].result.output == "C"

    # Combined using process info
    seq = SequentialCommand(
        [ProcessInfoCommand(), ProcessInfoCommand(), ProcessInfoCommand()],
        collect_outputs=True,
    )
    result = seq.run()
    assert result.succeeded is True
    # Process Ids should be identical since both are run in the same process
    assert result.output[0] == result.output[1] == result.output[2]


@pytest.mark.asyncio
async def test_sequential_async():
    # Simulate && with error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, collect_outputs=True)
    result = await seq.async_run()
    assert result.output == ["A", None]
    assert result.succeeded is False

    # Simulate ; in unix-like system
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
        ErrorCommand(raise_error=True),
        AddCharCommand("B"),
        ErrorCommand(raise_error=True),
    ]
    seq = SequentialCommand(commands, operator=None, collect_outputs=True)
    result = await seq.async_run()
    assert [cmd.result.succeeded for cmd in seq.commands] == [
        True,
        False,
        True,
        False,
        True,
        False,
    ]
    assert result.output == ["A", None, "C", None, "B", None]
    assert seq.commands[1].result.succeeded is False
    assert seq.commands[1].result.error is None
    assert isinstance(seq.commands[3].result.error, SystemError)
    assert seq.commands[5].result.succeeded is False
    assert isinstance(seq.commands[5].result.error, SystemError)

    # Test combined sequential commands with different operators
    # (A && B) || (C && D) = A and B
    cmd1 = SequentialCommand(
        [AddCharCommand("A"), AddCharCommand("B")], collect_outputs=True
    )
    cmd2 = SequentialCommand(
        [AddCharCommand("C"), AddCharCommand("D")], collect_outputs=True
    )
    seq = SequentialCommand([cmd1, cmd2], collect_outputs=True, operator="||")
    result = await seq.async_run()
    assert result.succeeded is True
    assert result.output == [["A", "B"]]

    # Test combined sequential commands
    # (A && Err) || (C && D) = C and D
    cmd1 = SequentialCommand(
        [AddCharCommand("A"), ErrorCommand(raise_error=True)],
        collect_outputs=True,
    )
    cmd2 = SequentialCommand(
        [AddCharCommand("C"), AddCharCommand("D")], collect_outputs=True
    )
    seq = SequentialCommand([cmd1, cmd2], operator="||")
    result = await seq.async_run()
    assert result.succeeded is True
    assert result.output == [["C", "D"]]

    # Test combined sequential commands
    # (Err || B) && (C && D) = B and C
    cmd1 = SequentialCommand(
        [ErrorCommand(raise_error=True), AddCharCommand("B")], operator="||"
    )
    cmd2 = SequentialCommand(
        [AddCharCommand("C"), AddCharCommand("D")], operator="||"
    )
    seq = SequentialCommand([cmd1, cmd2], collect_outputs=True)
    result = await seq.async_run()
    assert result.succeeded is True
    assert result.output == [["B"], ["C"]]


def test_parallel():
    # Run Parallel commands without error
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    par = ParallelCommand(commands, collect_outputs=True)
    result = par.run()
    assert result.output == ["A", "B", "C"]
    assert result.succeeded is True

    # Run Parallel commands with error, raise_error=False
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
        ErrorCommand(raise_error=True),
        AddCharCommand("B"),
        ErrorCommand(raise_error=False),
    ]
    par = ParallelCommand(commands, collect_outputs=True)
    result = par.run()
    assert result.succeeded is True
    assert result.output == ["A", None, "C", None, "B", None]

    # Run Parallel commands with error and raise_error=True
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
        ErrorCommand(raise_error=True),
        AddCharCommand("B"),
        ErrorCommand(raise_error=True),
    ]
    par = ParallelCommand(commands, collect_outputs=True)
    res = par.run()
    assert res.succeeded is True


def test_parallel_with_pid(in_vscode_launch):
    # Combined using process info
    seq = ParallelCommand(
        [ProcessInfoCommand(), ProcessInfoCommand(), ProcessInfoCommand()],
        number_of_processes=3,
        collect_outputs=True,
    )
    result = seq.run()
    assert result.succeeded is True

    # Process Ids are different since they are run in different processes.
    # If running in VSCode's debugger, the process Ids will be the same. So,
    # we check for the flag and assert the result accordingly.
    # If running outside the debugger, the process Ids will be different.
    # i.e. using `pytest` command in terminal.
    if in_vscode_launch:
        assert len(result.output) == 3
    else:
        assert result.output[0] != result.output[1] != result.output[2]
