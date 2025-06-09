import asyncio
import shutil
import subprocess
from asyncio.subprocess import Process


def check_which(cmd: str):
    if shutil.which(cmd) is None:
        raise FileNotFoundError(f"{cmd} not found")


async def check_which_async(cmd: str):
    return asyncio.to_thread(check_which, cmd)


def check_returncode(process: Process, command: list[str], stdout: bytes | None = None, stderr: bytes | None = None):
    if process.returncode != 0:
        returncode = process.returncode
        if returncode is None:
            returncode = 1
        raise subprocess.CalledProcessError(returncode, command, output=stdout, stderr=stderr)


async def run_process(command: list[str], check: bool, stdout: int = subprocess.PIPE, stderr: int = subprocess.PIPE):
    process = await asyncio.create_subprocess_exec(*command, stdout=stdout, stderr=stderr)
    ret_stdout, ret_stderr = await process.communicate()
    if check:
        check_returncode(process, command, ret_stdout, ret_stderr)
    return ret_stdout, ret_stderr


async def exec_process(command: list[str], stdout: int = subprocess.PIPE, stderr: int = subprocess.PIPE) -> Process:
    return await asyncio.create_subprocess_exec(*command, stdout=stdout, stderr=stderr)
