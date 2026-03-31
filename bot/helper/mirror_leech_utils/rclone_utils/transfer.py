from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs, listdir
from asyncio import create_subprocess_exec, wait_for
from asyncio.subprocess import PIPE
from configparser import RawConfigParser
from logging import getLogger
from random import randrange
from re import findall as re_findall

from ....core.config_manager import Config
from ...ext_utils.bot_utils import cmd_exec

LOGGER = getLogger(__name__)


class RcloneTransferHelper:
    def __init__(self, listener):
        self._listener = listener
        self._proc = None
        self._transferred_size = "0 B"
        self._eta = "-"
        self._percentage = "0%"
        self._speed = "0 B/s"
        self._size = "0 B"
        self._sa_count = 1
        self._sa_index = 0
        self._sa_number = 0
        self._use_service_accounts = Config.USE_SERVICE_ACCOUNTS

    @property
    def transferred_size(self):
        return self._transferred_size

    @property
    def percentage(self):
        return self._percentage

    @property
    def speed(self):
        return self._speed

    @property
    def eta(self):
        return self._eta

    @property
    def size(self):
        return self._size

    async def _progress(self):
        while not (
            self._proc.returncode is not None
            or self._proc.stdout.at_eof()
            or self._listener.is_cancelled
        ):
            try:
                data = await wait_for(self._proc.stdout.readline(), 60)
            except:
                break
            if not data:
                break
            data = data.decode().strip()
            if data := re_findall(
                r"Transferred:\s+([\d.]+\s*\w+)\s+/\s+([\d.]+\s*\w+),\s+([\d.]+%)\s*,\s+([\d.]+\s*\w+/s),\s+ETA\s+([\dwdhms]+)",
                data,
            ):
                (
                    self._transferred_size,
                    self._size,
                    self._percentage,
                    self._speed,
                    self._eta,
                ) = data[0]

    def _switch_service_account(self):
        if self._sa_index == self._sa_number - 1:
            self._sa_index = 0
        else:
            self._sa_index += 1
        self._sa_count += 1
        remote = f"sa{self._sa_index:03}"
        LOGGER.info(f"Switching to {remote} remote")
        return remote

    async def _create_rc_sa(self, remote, remote_opts):
        sa_conf_dir = "rclone_sa"
        sa_conf_file = f"{sa_conf_dir}/{remote}.conf"
        if await aiopath.isfile(sa_conf_file):
            return sa_conf_file
        await makedirs(sa_conf_dir, exist_ok=True)

        if gd_id := remote_opts.get("team_drive"):
            option = "team_drive"
        elif gd_id := remote_opts.get("root_folder_id"):
            option = "root_folder_id"
        else:
            self._use_service_accounts = False
            return "rclone.conf"

        files = await listdir("accounts")
        text = "".join(
            f"[sa{i:03}]\ntype = drive\nscope = drive\nservice_account_file = accounts/{sa}\n{option} = {gd_id}\n\n"
            for i, sa in enumerate(files)
        )

        async with aiopen(sa_conf_file, "w") as f:
            await f.write(text)
        return sa_conf_file

    async def _start_download(self, cmd, remote_type):
        self._proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        await self._progress()
        _, stderr = await self._proc.communicate()
        return_code = self._proc.returncode
        if self._listener.is_cancelled:
            return

        if return_code == 0:
            await self._listener.on_download_complete()
        elif return_code != -9:
            error = stderr.decode().strip()
            if not error and remote_type == "drive" and self._use_service_accounts:
                error = "Mostly your service accounts don't have access to this drive!"
            LOGGER.error(error)

            if (
                self._sa_number != 0
                and remote_type == "drive"
                and "RATE_LIMIT_EXCEEDED" in error
                and self._use_service_accounts
            ):
                if self._sa_count < self._sa_number:
                    remote = self._switch_service_account()
                    cmd[6] = f"{remote}:{cmd[6].split(':', 1)[1]}"
                    if self._listener.is_cancelled:
                        return
                    return await self._start_download(cmd, remote_type)
                else:
                    LOGGER.info(
                        f"Reached maximum number of service accounts switching, which is {self._sa_count}"
                    )

            await self._listener.on_download_error(error[:4000])
            return

    async def download(self, remote, config_path, path):
        try:
            remote_opts = await self._get_remote_options(config_path, remote)
        except Exception as err:
            await self._listener.on_download_error(str(err))
            return
        remote_type = remote_opts["type"]

        if (
            remote_type == "drive"
            and self._use_service_accounts
            and config_path == "rclone.conf"
            and await aiopath.isdir("accounts")
            and not remote_opts.get("service_account_file")
        ):
            config_path = await self._create_rc_sa(remote, remote_opts)
            if config_path != "rclone.conf":
                sa_files = await listdir("accounts")
                self._sa_number = len(sa_files)
                self._sa_index = randrange(self._sa_number)
                remote = f"sa{self._sa_index:03}"
                LOGGER.info(f"Download with service account {remote}")

        cmd = self._get_updated_command(
            config_path, f"{remote}:{self._listener.link}", path, "copy"
        )

        if remote_type == "drive" and not self._listener.rc_flags:
            cmd.extend(
                (
                    "--drive-acknowledge-abuse",
                    "--drive-chunk-size",
                    "128M",
                    "--tpslimit",
                    "1",
                    "--tpslimit-burst",
                    "1",
                    "--transfers",
                    "1",
                )
            )

        await self._start_download(cmd, remote_type)

    def _get_updated_command(
        self,
        config_path,
        source,
        destination,
        method,
    ):
        rclone_select = False
        if source.split(":")[-1].startswith("rclone_select"):
            source = f"{source.split(":")[0]}:"
            rclone_select = True
        cmd = [
            "rclone",
            method,
            "--fast-list",
            "--config",
            config_path,
            "-P",
            source,
            destination,
            "-L",
            "--retries-sleep",
            "3s",
            "--ignore-case",
            "--low-level-retries",
            "1",
            "-M",
        ]
        if rclone_select:
            cmd.extend(("--files-from", self._listener.link))
        elif self._listener.included_extensions:
            ext = "*.{" + ",".join(self._listener.included_extensions) + "}"
            cmd.extend(("--include", ext))
        else:
            ext = "*.{" + ",".join(self._listener.excluded_extensions) + "}"
            cmd.extend(("--exclude", ext))
        if rcflags := self._listener.rc_flags:
            rcflags = rcflags.split("|")
            for flag in rcflags:
                if ":" in flag:
                    key, value = map(str.strip, flag.split(":", 1))
                    cmd.extend((key, value))
                elif len(flag) > 0:
                    cmd.append(flag.strip())
        return cmd

    @staticmethod
    async def _get_remote_options(config_path, remote):
        config = RawConfigParser()
        async with aiopen(config_path, "r") as f:
            contents = await f.read()
            config.read_string(contents)
        options = config.options(remote)
        return {opt: config.get(remote, opt) for opt in options}

    async def cancel_task(self):
        self._listener.is_cancelled = True
        if self._proc is not None:
            try:
                self._proc.kill()
            except:
                pass
        LOGGER.info(f"Cancelling Download: {self._listener.name}")
        await self._listener.on_download_error("Stopped by user!")
