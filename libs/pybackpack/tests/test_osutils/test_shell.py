import pytest
from pybackpack.osutils.shell import ProcessCommand, run_command, find
from pybackpack.commands import (
    PipeCommand,
    SequentialCommand,
)

# pylint: disable=redefined-outer-name


@pytest.fixture
def temp_dir(tmpdir):
    # Create dir1 with some sample files and directories

    dir1 = tmpdir.mkdir("dir1")
    dir1.join("file1.yml").write("content")
    dir1.join("file2.yaml").write("content")
    dir1.join("file1.dev.yml").write("content")
    dir1.join("file2.dev.yaml").write("content")
    dir1.join("file3.txt").write("content")
    dir1.join("file4.py").write("content")
    dir1.join("file5.yamld").write("content")

    # Add a subdirectory
    dir1_sub1 = dir1.mkdir("dir1_sub1")
    dir1_sub1.join("file1.txt").write("content")
    dir1_sub1.join("file2.txt").write("content")

    return tmpdir


def test_shell_command():
    cmd = ProcessCommand(["echo", "Hello"])
    res = cmd.run()
    assert res.output == "Hello\n"
    assert res.metadata.returncode == 0

    # Access result from cmd object
    assert cmd.result.metadata.returncode == 0
    assert cmd.result.metadata.stdout == "Hello\n"

    # Test with env variables
    cmd = ProcessCommand(["env"], env={"MY_VAR": "test1"})
    res = cmd.run()
    assert "MY_VAR=test1" in res.output

    # Test long running command
    cmd = ProcessCommand(["sleep", "2"], timeout=1)
    res = cmd.run()
    assert res.output is None
    assert res.succeeded is False
    assert cmd.failed_with_time_out is True

    # Test failed command
    cmd = ProcessCommand(["ls", "unknown"])
    res = cmd.run()
    assert res.output is None
    assert res.succeeded is False
    assert res.error.returncode == 2
    assert "No such file or directory" in res.error.stderr

    # Test failed command with ignore_errors=False. It will raise an exception
    cmd = ProcessCommand(["ls", "unknown"], raise_error=True)
    with pytest.raises(Exception):
        cmd.run()
    assert cmd.result.succeeded is False
    assert cmd.result.error.returncode == 2

    # Test failed with command not found
    cmd = ProcessCommand(["unknown"])
    res = cmd.run()
    assert res.output is None
    assert res.error.errno == 2
    assert cmd.failed_with_command_not_found is True
    assert "No such file or directory" in res.error.strerror

    # Multiline output
    cmd = ProcessCommand(["ls", "/", "-l"])
    res = cmd.run()
    assert res.output is not None
    assert len(res.output.splitlines()) > 1


def test_pipe():
    # Pipe commands
    commands = [
        ProcessCommand(["echo", "Hello World"]),
        ProcessCommand(["cut", "-d", " ", "-f", "1"]),
        ProcessCommand(["awk", "{print $1}"]),
    ]
    pipe = PipeCommand(commands)
    res = pipe.run()
    assert res.succeeded is True
    assert res.output == "Hello\n"
    assert pipe.commands[0].result.metadata.returncode == 0
    assert pipe.commands[0].result.metadata.stdout == "Hello World\n"
    assert pipe.commands[1].result.metadata.returncode == 0
    assert pipe.commands[2].result.metadata.returncode == 0

    # Test with a failing command in the middle
    commands = [
        ProcessCommand(["echo", "Hello World"]),
        ProcessCommand(["cut", "-d", " ", "-f", "1"]),
        ProcessCommand(["unknown"]),
        ProcessCommand(["awk", "{print $1}"]),
    ]
    pipe = PipeCommand(commands)
    res = pipe.run()
    assert res.output is None
    assert res.succeeded is False
    assert pipe.commands[0].result.succeeded is True
    assert pipe.commands[0].result.output == "Hello World\n"
    assert pipe.commands[1].result.succeeded is True
    assert pipe.commands[1].result.output == "Hello\n"
    assert pipe.commands[2].result.succeeded is False
    assert pipe.commands[2].result.output is None
    assert pipe.commands[3].result is None


def test_sequential():
    # Simulate && operator in shell
    commands = [
        ProcessCommand(["echo", "Hello"]),
        ProcessCommand(["echo", "World"]),
    ]
    seq = SequentialCommand(commands)
    res = seq.run()
    assert {"Hello\n", "World\n"} == set(res.output)

    # Simulate && with error
    commands = [
        ProcessCommand(["echo", "Hello"]),
        ProcessCommand(["ls", "unknown"]),
        ProcessCommand(["echo", "World"]),
    ]
    seq = SequentialCommand(commands)
    res = seq.run()
    assert ["Hello\n", None] == res.output
    assert res.succeeded is False
    # resul of the first command
    assert seq.commands[0].result.metadata.returncode == 0
    assert seq.commands[0].result.metadata.stdout == "Hello\n"
    # The command which failed
    assert seq.commands[1].result.succeeded is False
    assert seq.commands[1].result.error.returncode == 2
    assert "No such file or directory" in seq.commands[1].result.error.stderr

    # Simulate || operator in shell
    commands = [
        ProcessCommand(["echo", "Hello"]),
        ProcessCommand(["ls", "unknown"]),
        ProcessCommand(["echo", "World"]),
    ]
    seq = SequentialCommand(commands, operator="||")
    result = seq.run()
    assert result.output == ["Hello\n"]

    # Simulatte ; operator in shell
    commands = [
        ProcessCommand(["echo", "Hello"]),
        ProcessCommand(["ls", "unknown"]),
        ProcessCommand(["echo", "World"]),
    ]
    seq = SequentialCommand(commands, operator=None)
    result = seq.run()
    assert ["Hello\n", None, "World\n"] == result.output

    # Invalid operator
    with pytest.raises(ValueError):
        SequentialCommand(commands, operator="invalid").run()


def test_sequential_combined():
    # Sequential combined
    cmd1 = SequentialCommand(
        [
            ProcessCommand(["echo", "1"]),
            ProcessCommand(["python3", "-c", "import os; print(os.getpid())"]),
        ]
    )
    cmd2 = SequentialCommand(
        [
            ProcessCommand(["echo", "2"]),
            ProcessCommand(["python3", "-c", "import os; print(os.getpid())"]),
        ]
    )
    cmd3 = SequentialCommand(
        [
            ProcessCommand(["echo", "3"]),
            ProcessCommand(["python3", "-c", "import os; print(os.getpid())"]),
        ]
    )

    # Run the commands in sequence
    seq = SequentialCommand([cmd1, cmd2, cmd3])
    result = seq.run()
    assert len(result.output) == 3


def test_run_command():
    # Test multi-lines output
    cmd = ["ls", "/", "-l"]
    res = run_command(cmd)
    assert len(res) > 1

    # Test failure
    cmd = ["ls", "unknown"]
    try:
        run_command(cmd)
        assert False
    except Exception as ex:
        # pylint: disable=no-member
        assert "No such file or directory" in str(ex.stderr)


def test_find(temp_dir):
    # Get all yaml files
    files = find(temp_dir, names=["*.yaml", "*.yml"])
    assert len(files) == 4
    assert {"file3.txt", "file4.py"} not in set(files)

    # Get all the yaml files using regex
    files = find(temp_dir, names=[r".*\.ya?ml$"], use_regex=True)
    assert len(files) == 4
    assert {"file3.txt", "file4.py"} not in set(files)

    # Get all the files except txt and py files
    files = find(temp_dir, types=["f"], exclude_names=["*.txt", "*.py"])
    assert len(files) == 5
    assert {"file3.txt", "file4.py"} not in set(files)

    # Get all the yaml files except the ones with dev in the name
    files = find(
        temp_dir,
        names=[r".*\.ya?ml$"],
        exclude_names=[r".*dev.ya?ml$"],
        use_regex=True,
    )
    assert len(files) == 2
    assert {"file1.dev.yml", "file2.dev.yaml"} not in set(files)

    # Finding no files with the given patterns
    files = find(temp_dir, names=["*.cpp"])
    assert len(files) == 0

    # All *.txt files in all directories
    files = find(temp_dir, names=["*.txt"])
    assert len(files) == 3

    # Find a particular file
    file_name = "file3"
    files = find(temp_dir, names=[rf"{file_name}\.txt"])
    assert len(files) == 1
