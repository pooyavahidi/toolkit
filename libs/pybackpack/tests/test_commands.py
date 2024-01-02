import os
import pytest
from pybackpack.commands import (
    Command,
    CommandResult,
    PipeCommand,
    SequentialCommand,
    MultiProcessCommand,
    run_command,
    async_run_command,
)

# pylint: disable=missing-class-docstring,too-few-public-methods


@pytest.fixture
def in_vscode_launch(request):
    """Return True if running in VSCode's debugger."""
    return request.config.getoption("--vscode-launch", default=False)


class AddCharCommand(Command):
    def __init__(self, char=None) -> None:
        self.char = char

    def run(self, input_data=None):
        if not input_data:
            input_data = ""
        return CommandResult(f"{input_data}{self.char}")

    async def async_run(self, input_data=None) -> CommandResult:
        return self.run(input_data)


class ErrorCommand(Command):
    def __init__(self, raise_error=False) -> None:
        self.error_message = "Error from ErrorCommand"
        self.raise_error = raise_error

    def run(self, input_data=None):
        if self.raise_error:
            raise SystemError(self.error_message)

        return CommandResult(
            output=None,
            succeeded=False,
            error_message=self.error_message,
        )

    async def async_run(self, input_data=None) -> CommandResult:
        return self.run()


class ProcessInfoCommand(Command):
    def run(self, input_data=None):
        return CommandResult(output=os.getpid())

    async def async_run(self, input_data=None) -> CommandResult:
        return self.run()


def test_single_command():
    cmd = AddCharCommand("A")
    res = run_command(cmd)
    assert res.output == "A"
    assert res.succeeded is True

    # Command with error
    cmd = ErrorCommand(raise_error=True)
    res = run_command(cmd)
    assert res.succeeded is False
    assert res.output is None
    assert isinstance(res.error, SystemError)
    assert res.error_message == "Error from ErrorCommand"

    # Command with error which doesn't raise error
    cmd = ErrorCommand(raise_error=False)
    res = run_command(cmd)
    assert res.output is None
    assert res.succeeded is False
    assert res.error_message == "Error from ErrorCommand"

    # Test always raising error command
    cmd = ErrorCommand(raise_error=True)
    res = run_command(cmd)
    assert res.succeeded is False
    assert res.output is None
    assert isinstance(res.error, SystemError)

    # Pass None as input
    cmd = AddCharCommand("A")
    res = run_command(cmd, input_data=None)
    assert res.output == "A"
    assert res.succeeded is True

    # Pass empty string as input
    cmd = AddCharCommand("A")
    res = run_command(cmd, input_data="")
    assert res.output == "A"
    assert res.succeeded is True


@pytest.mark.asyncio
async def test_single_command_async():
    # Command without error
    cmd = AddCharCommand(char="A")
    res = await async_run_command(cmd)
    assert res.output == "A"
    assert res.succeeded is True

    # Command which raises error
    cmd = ErrorCommand(raise_error=True)
    res = await async_run_command(cmd)
    assert res.succeeded is False
    assert res.output is None
    assert isinstance(res.error, SystemError)
    assert res.error_message == "Error from ErrorCommand"

    # Command with error which doesn't raise error
    cmd = ErrorCommand(raise_error=False)
    res = await async_run_command(cmd)
    assert res.output is None
    assert res.succeeded is False
    assert res.error_message == "Error from ErrorCommand"


def test_pipe():
    # Test pipe with simple commands, no errors
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands=commands)
    res = run_command(pipe)
    assert res.output == "ABC"

    # Test pipe with initial input to the pipe
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    res = run_command(pipe, input_data="D")
    assert res.output == "DABC"
    assert len(res.results) == 3
    assert res.results[0].output == "DA"
    assert res.results[0].succeeded is True
    assert res.results[1].output == "DAB"
    assert res.results[2].output == "DABC"

    # Test pipe with command with error which doesn't raise error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    res = run_command(pipe)
    assert res.output is None
    assert res.succeeded is False
    assert res.error_message == "Error from ErrorCommand"
    # The error is None as Command doesn't raise error
    assert res.error is None
    assert res.results[0].output == "A"

    # Test pipe with command with error which raises error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=True),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    res = run_command(pipe)
    assert res.succeeded is False
    assert res.output is None
    assert isinstance(res.error, SystemError)

    # Test pipe with None commands
    with pytest.raises(ValueError):
        pipe = PipeCommand(commands=None)

    # Test pipe with empty commands
    with pytest.raises(ValueError):
        pipe = PipeCommand(commands=[])


def test_pipe_collect_results():
    # Test without collecting results
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands, collect_results=False)
    res = run_command(pipe)
    assert res.output is None
    assert res.succeeded is False
    assert res.error_message == "Error from ErrorCommand"
    assert res.results == []


@pytest.mark.asyncio
async def test_pipe_async():
    # Test pipe with simple commands, no errors
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    res = await async_run_command(pipe)
    assert res.succeeded is True
    assert res.output == "ABC"

    # Test pipe with initial input to the pipe
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    res = await async_run_command(pipe, input_data="D")
    assert res.output == "DABC"
    assert res.results[0].output == "DA"
    assert res.results[0].succeeded is True
    assert res.results[1].output == "DAB"
    assert res.results[2].output == "DABC"

    # Test pipe with command with error which doesn't raise error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    res = await async_run_command(pipe)
    assert res.output is None
    assert res.succeeded is False
    assert res.error_message == "Error from ErrorCommand"
    assert res.error is None
    assert res.results[0].output == "A"

    # Test pipe with command with error which raises error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=True),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands)
    res = await async_run_command(pipe)
    assert res.succeeded is False
    assert res.output is None
    assert isinstance(res.error, SystemError)

    # Test without collecting results
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    pipe = PipeCommand(commands, collect_results=False)
    res = await async_run_command(pipe)
    assert res.output is None
    assert res.succeeded is False
    assert res.error_message == "Error from ErrorCommand"


def test_sequential_and_operator():
    # Simulate && in unix-like systems
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands)
    res = run_command(seq)
    assert res.succeeded is True
    assert res.output == ["A", "B", "C"]
    # It can also be achieved using `results` attribute as both are the same.
    # `results` holds all the additional information about the command
    # execution such as error, output, etc.
    outputs = [r.output for r in res.results]
    assert outputs == ["A", "B", "C"]

    # Simulate && with error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands)
    res = run_command(seq)
    assert res.output == ["A", None]
    assert res.succeeded is False

    # Simulate && with error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands)
    res = run_command(seq)
    assert res.output == ["A", None]
    assert res.succeeded is False

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
    res = run_command(seq)
    assert res.output == ["A"]

    # Simulate || with error early
    commands = [
        ErrorCommand(raise_error=False),
        ErrorCommand(raise_error=True),
        AddCharCommand("A"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, operator="||")
    res = run_command(seq)
    assert set(res.output) >= {"A"}
    assert res.succeeded is True
    assert res.results[0].output is None
    assert isinstance(res.results[1].error, SystemError)
    assert res.results[2].output == "A"

    # Simulate || without error
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, operator="||")
    res = run_command(seq)
    assert res.output == ["A"]
    assert res.succeeded is True

    # Simulate || with collecting outputs
    commands = [
        ErrorCommand(raise_error=False),
        AddCharCommand("A"),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands, operator="||")
    res = run_command(seq)
    assert res.output == [None, "A"]
    assert res.succeeded is True
    assert res.results[0].output is None
    assert res.results[1].output == "A"
    assert len(res.results) == 2


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
    seq = SequentialCommand(commands, operator=None)
    res = run_command(seq)
    assert [res.succeeded for res in res.results] == [
        True,
        False,
        True,
        False,
        True,
        False,
    ]
    assert res.output == ["A", None, "C", None, "B", None]
    assert res.results[1].succeeded is False
    assert res.results[1].error is None
    assert isinstance(res.results[3].error, SystemError)
    assert res.results[5].succeeded is False
    assert isinstance(res.results[5].error, SystemError)


def test_sequential_combined():
    # (A && B) && (C && D) = A && B && C && D
    seq1 = SequentialCommand([AddCharCommand("A"), AddCharCommand("B")])
    seq2 = SequentialCommand([AddCharCommand("C"), AddCharCommand("D")])
    seq = SequentialCommand([seq1, seq2])
    res = run_command(seq)
    assert res.output == [["A", "B"], ["C", "D"]]
    assert res.succeeded is True
    assert res.results[0].output == ["A", "B"]
    # Directly accessing the output of the first command in seq2
    assert res.results[1].results[0].output == "C"
    # Directly access the outputs using the `output` attribute
    assert res.output[1][0] == "C"

    # (A && B) || (C && D)
    seq1 = SequentialCommand([AddCharCommand("A"), AddCharCommand("B")])
    seq2 = SequentialCommand([AddCharCommand("C"), AddCharCommand("D")])
    seq = SequentialCommand([seq1, seq2], operator="||")
    res = run_command(seq)
    assert res.output == [["A", "B"]]
    assert res.succeeded is True
    assert res.results[0].output == ["A", "B"]
    assert len(res.results) == 1

    # Combined using process info
    seq = SequentialCommand(
        [ProcessInfoCommand(), ProcessInfoCommand(), ProcessInfoCommand()],
    )
    res = run_command(seq)
    assert res.succeeded is True
    # Process Ids should be identical since both are run in the same process
    assert res.output[0] == res.output[1] == res.output[2]


@pytest.mark.asyncio
async def test_sequential_async():
    # Simulate && with error
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
    ]
    seq = SequentialCommand(commands)
    res = await async_run_command(seq)
    assert res.output == ["A", None]
    assert res.succeeded is False

    # Simulate ; in unix-like system
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
        ErrorCommand(raise_error=True),
        AddCharCommand("B"),
        ErrorCommand(raise_error=True),
    ]
    seq = SequentialCommand(commands, operator=None)
    res = await async_run_command(seq)
    assert [res.succeeded for res in res.results] == [
        True,
        False,
        True,
        False,
        True,
        False,
    ]
    assert res.output == ["A", None, "C", None, "B", None]
    assert res.results[1].succeeded is False
    assert res.results[1].error is None
    assert isinstance(res.results[3].error, SystemError)
    assert res.results[5].succeeded is False
    assert isinstance(res.results[5].error, SystemError)

    # Test combined sequential commands with different operators
    # (A && B) || (C && D) = A and B
    seq1 = SequentialCommand([AddCharCommand("A"), AddCharCommand("B")])
    seq2 = SequentialCommand([AddCharCommand("C"), AddCharCommand("D")])
    seq = SequentialCommand([seq1, seq2], operator="||")
    res = await async_run_command(seq)
    assert res.succeeded is True
    assert res.output == [["A", "B"]]

    # Test combined sequential commands
    # (A && Err) || (C && D) = C and D
    seq1 = SequentialCommand(
        [AddCharCommand("A"), ErrorCommand(raise_error=True)]
    )
    seq2 = SequentialCommand([AddCharCommand("C"), AddCharCommand("D")])
    seq = SequentialCommand([seq1, seq2], operator="||")
    res = await async_run_command(seq)
    assert res.succeeded is True
    assert res.output == [["A", None], ["C", "D"]]
    # Last output
    assert res.output[-1] == ["C", "D"]
    # Last result
    assert res.results[-1].output == ["C", "D"]

    # Test combined sequential commands
    # (Err || B) && (C && D) = B and C
    seq1 = SequentialCommand(
        [ErrorCommand(raise_error=True), AddCharCommand("B")],
        operator="||",
    )
    seq2 = SequentialCommand(
        [AddCharCommand("C"), AddCharCommand("D")],
        operator="||",
    )
    seq = SequentialCommand([seq1, seq2])
    res = await async_run_command(seq)
    assert res.succeeded is True
    assert res.output == [[None, "B"], ["C"]]


def test_sequential_collect_results():
    seq1 = SequentialCommand(
        [AddCharCommand("A"), ErrorCommand(raise_error=True)]
    )
    seq2 = SequentialCommand([AddCharCommand("C"), AddCharCommand("D")])
    seq = SequentialCommand([seq1, seq2], operator="||", collect_results=False)
    res = run_command(seq)
    assert res.succeeded is True
    assert res.output == []
    assert res.results == []

    # Partial collection of results
    seq1 = SequentialCommand(
        [AddCharCommand("A"), ErrorCommand(raise_error=True)]
    )
    seq2 = SequentialCommand(
        [AddCharCommand("C"), AddCharCommand("D")], collect_results=False
    )
    seq = SequentialCommand([seq1, seq2], operator="||")
    res = run_command(seq)
    assert res.succeeded is True
    assert res.output == [["A", None], []]
    assert len(res.results) == 2
    assert res.results[0].output == ["A", None]
    assert res.results[1].output == []


def test_parallel():
    # Run Parallel commands without error
    commands = [
        AddCharCommand("A"),
        AddCharCommand("B"),
        AddCharCommand("C"),
    ]
    par = MultiProcessCommand(commands)
    res = run_command(par)
    assert res.output == ["A", "B", "C"]
    assert res.succeeded is True
    assert par._results[0].output == "A"

    # Run Parallel commands with error, raise_error=False
    commands = [
        AddCharCommand("A"),
        ErrorCommand(raise_error=False),
        AddCharCommand("C"),
        ErrorCommand(raise_error=True),
        AddCharCommand("B"),
        ErrorCommand(raise_error=False),
    ]
    par = MultiProcessCommand(commands)
    res = run_command(par)
    assert res.succeeded is True
    assert res.output == ["A", None, "C", None, "B", None]


def test_parallel_with_pid(in_vscode_launch):
    # Combined using process info
    seq = MultiProcessCommand(
        [ProcessInfoCommand(), ProcessInfoCommand(), ProcessInfoCommand()],
        pool_size=3,
    )
    res = run_command(seq)
    assert res.succeeded is True

    # Process Ids are different since they are run in different processes.
    # If running in VSCode's debugger, the process Ids will be the same. So,
    # we check for the flag and assert the result accordingly.
    # If running outside the debugger, the process Ids will be different.
    # i.e. using `pytest` command in terminal.
    if in_vscode_launch:
        assert len(res.output) == 3
    else:
        assert res.output[0] != res.output[1] != res.output[2]
