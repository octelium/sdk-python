"""Attach to an existing workspace: python terminal.py abc. Removes the example PTY."""

import argparse

from cordium import Cordium


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspace")
    args = parser.parse_args()
    with Cordium() as client:
        workspace = client.workspaces.get(args.workspace)
        terminal = workspace.terminals.create(cols=100, rows=30)
        try:
            with terminal:
                terminal.write("printf 'Hello from a persistent PTY\\n'\n")
                with terminal.events as events:
                    for event in events:
                        if event.type == "output":
                            print(event.data.decode("utf-8", errors="replace"), end="")
                            break
        finally:
            terminal.remove()


if __name__ == "__main__":
    main()
