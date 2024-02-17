import subprocess

from argparse import Namespace
from yaml import safe_load
from mergedeep import merge


def exec_cmd(args: list) -> str:
    # Exec input command
    out = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    if out.returncode != 0:
        stdout_msg = stdout.decode('utf-8') if stdout is not None else ''
        stderr_msg = stderr.decode('utf-8') if stderr is not None else ''
        print(out.returncode)
        raise Exception(f"Command returned code {out.returncode}. Stdout: '{stdout_msg}' Stderr: '{stderr_msg}'")

    return stdout.decode("utf-8")


def set_confs(args: Namespace) -> dict:
    confs = {}
    if args.config != "":
        with open(args.config) as conf:
            confs = safe_load(conf)

    confs = merge(args.__dict__, confs)

    return confs
