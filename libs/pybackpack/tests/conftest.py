def pytest_addoption(parser):
    parser.addoption(
        "--vscode-launch",
        action="store_true",
        default=False,
        help="Indicates if tests are being run from VSCode's debugger",
    )
