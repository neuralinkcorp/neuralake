#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class StaticSiteBuildHook(BuildHookInterface):
    PLUGIN_NAME = "static_site"

    def initialize(self, version, build_data):
        """Run before the build process starts"""
        script_dir = Path(__file__).parent.absolute()

        os.chdir(script_dir)

        try:
            # Check if node is installed
            try:
                subprocess.run(
                    ["node", "--version"], check=True, capture_output=True, text=True
                )
            except (subprocess.CalledProcessError, FileNotFoundError):
                raise RuntimeError(
                    "Node.js is not installed or not in PATH. Please install Node.js to build the static site."
                )

            print("Running npm install...")
            result = subprocess.run(
                ["npm", "install", "--legacy-peer-deps"],
                check=True,
                capture_output=True,
                text=True,
            )
            print(result.stdout)

            print("Running npm build...")
            result = subprocess.run(
                ["npm", "run", "build"], check=True, capture_output=True, text=True
            )
            print(result.stdout)

            precompiled_dir = script_dir / "precompiled"
            if not precompiled_dir.exists():
                print("Error: precompiled directory not found after build")
                raise RuntimeError("precompiled directory not found after build")

            print("Static site build completed successfully")

        except subprocess.CalledProcessError as e:
            print(f"Error running npm command: {e.stderr}", file=sys.stderr)
            raise RuntimeError(f"npm command failed: {e.stderr}")
        except Exception as e:
            print(f"Unexpected error: {str(e)}", file=sys.stderr)
            raise
