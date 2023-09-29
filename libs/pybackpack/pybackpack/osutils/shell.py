import os
import subprocess
from typing import List, Optional
from pybackpack.commands import Command, CommandResult


# pylint: disable=too-many-arguments,too-many-instance-attributes
class ProcessCommand(Command):
    """This class is a wrapper around subprocess.run() to run shell commands.

    The result of the command is stored in the `result` attribute. Also the
    stdout of the command is returned by the `run()` method.
    Attributes:
        cmd (list): The command to run.
        capture_output (bool): Capture stdout and stderr.
        check (bool): If True, raise an exception if the command fails.
        text (bool): If True, stdout and stderr are returned as strings.
        timeout (int): The timeout for the command.
        cwd (str): The current working directory.
        shell (bool): If True, run the command in a shell.
        encoding (str): The encoding of the command output.
        errors (str): The error handling of the command output.
        stdin (int): The stdin of the command.
        input_data (str): The input for the command.
        env (dict): The environment variables for the command.
        inherit_env (bool): If True, use the current environment as the base.
        other_popen_kwargs (dict): Keyword arguments for subprocess.Popen.
    """

    def __init__(
        self,
        cmd,
        capture_output=True,
        check=True,
        text=True,
        encoding="utf-8",
        timeout=None,
        cwd=None,
        shell=False,
        errors=None,
        stdin=None,
        input_data=None,
        env=None,
        inherit_env=True,
        raise_error=False,
        **other_popen_kwargs,
    ):
        super().__init__(input_data=input_data, raise_error=raise_error)

        if inherit_env:
            # Use the current environment as the base
            self.env = os.environ.copy()
            if env:
                # Update with provided env variables
                self.env.update(env)
        else:
            self.env = env

        self.cmd = cmd
        self.capture_output = capture_output
        self.check = check
        self.text = text
        self.timeout = timeout
        self.cwd = cwd
        self.shell = shell
        self.encoding = encoding
        self.errors = errors
        self.stdin = stdin
        self.other_popen_kwargs = other_popen_kwargs

        # Failed flags
        self.failed_with_time_out = False
        self.failed_with_command_not_found = False

    def _run(self) -> CommandResult:
        try:
            result = subprocess.run(
                self.cmd,
                capture_output=self.capture_output,
                check=self.check,
                text=self.text,
                timeout=self.timeout,
                cwd=self.cwd,
                env=self.env,
                shell=self.shell,
                encoding=self.encoding,
                errors=self.errors,
                stdin=self.stdin,
                input=self.input_data,
                **self.other_popen_kwargs,
            )
            return CommandResult(
                output=result.stdout,
                metadata=result,
            )

        except Exception as ex:
            if isinstance(ex, subprocess.TimeoutExpired):
                self.failed_with_time_out = True
            if isinstance(ex, FileNotFoundError):
                self.failed_with_command_not_found = True

            raise ex


def run_command(cmd: List[str], **kwargs) -> List[str]:
    """Run a command and return the output as a list of lines. This is a
    simple wrapper around ProcessCommand. For more control over the command
    execution, or use it with pipes, sequenes, etc. use ProcessCommand
    directly.

    Args:
        cmd (list): The command to run.
        kwargs: Keyword arguments for ProcessCommand.
    Returns:
        list: A list of lines from the command output.
    """
    process_cmd = ProcessCommand(cmd, **kwargs)
    result = process_cmd.run()

    if not result.succeeded:
        raise result.error

    if not result.output:
        return []

    # Use strip() twice to remove empty lines
    return [
        line.strip() for line in result.output.splitlines() if line.strip()
    ]


def find(
    directory: str,
    names: Optional[List[str]] = None,
    exclude_names: Optional[List[str]] = None,
    types: Optional[List[str]] = None,
    use_regex: bool = False,
) -> List[str]:
    """Wrapper around the find command in Unix-like systems.
    Args:
        directory (str): The directory to search.
        name (list): A list of object names to search for.
        exclude_name (list): A list of object names to exclude.
        types (list): A list of object types to search for.
            The common types are: f (file), d (directory), l (symbolic link).
            For the complete list of possible options see `find` command help.
        use_regex (bool): Use regex for file name search. Default is False.
            if False, use glob patterns for names.
    Returns:
        list: A list of objects found.
    """

    # A simple AST-like template for constructing the find command
    ast = {
        "base": ["find", "{}"],
        "name": ["-name", "{}"] if not use_regex else ["-regex", "{}"],
        "type": ["-type", "{}"],
        "or": "-o",
        "not": "!",
        "open_paren": "(",
        "close_paren": ")",
    }

    cmd = ast["base"].copy()
    cmd[1] = cmd[1].format(directory)

    # Handle object types
    if types:
        cmd.append(ast["open_paren"])
        for otype in types:
            cmd.extend(ast["type"])
            cmd[-1] = cmd[-1].format(otype)
            cmd.append(ast["or"])
        cmd.pop()  # remove the last '-o'
        cmd.append(ast["close_paren"])

    # Handle object names
    if names:
        for n in names:
            cmd.extend(ast["name"])
            cmd[-1] = cmd[-1].format(n)
            cmd.append(ast["or"])
        cmd.pop()  # remove the last '-o'

    # Handle excluded names
    if exclude_names:
        cmd.append(ast["not"])
        cmd.append(ast["open_paren"])
        for ex in exclude_names:
            cmd.extend(ast["name"])
            cmd[-1] = cmd[-1].format(ex)
            cmd.append(ast["or"])
        cmd.pop()  # remove the last '-o'
        cmd.append(ast["close_paren"])

    return run_command(cmd)
