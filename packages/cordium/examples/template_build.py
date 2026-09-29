"""Build an existing template: python template_build.py python.team.cordium."""

import argparse

from cordium import Cordium


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("template")
    args = parser.parse_args()
    with Cordium() as client:
        template = client.templates.build(args.template, tags=("python-sdk-example",))
        build_id = template.status.build_info.current_running_build_id
        if not build_id:
            raise RuntimeError("Cluster did not return the started build ID")
        client.templates.wait_for_build(args.template, build_id, timeout=900)
        print("Ready:", build_id)


if __name__ == "__main__":
    main()
