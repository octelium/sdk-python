"""Run with CORDIUM_DOMAIN and an OCTELIUM credential variable configured."""

from cordium import Cordium, Resources


def main() -> None:
    with Cordium() as client:
        workspace = client.workspaces.create(
            image="python:3.13", resources=Resources(cpu=1000, memory=1024), ephemeral=True
        )
        try:
            workspace.start().wait_until_running()
            print(workspace.exec(["python", "-c", "print('Hello from Cordium')"]).stdout)
            workspace.files.write_text("/workspace/hello.txt", "Hello!\n")
            print(workspace.files.read_text("/workspace/hello.txt"))
        finally:
            if not workspace.refresh().is_stopped:
                workspace.stop().wait_until_stopped()
            workspace.delete()


if __name__ == "__main__":
    main()
