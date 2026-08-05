"""
_atomic.py: durable writes for the files this package leaves on disk.

Every artifact here is something a third party reads back: claims.toml is
the disaster-recovery copy of the graph, a private key is the identity a
project signs with, an export is what a reviewer verifies. Writing one in
place truncates the target before the first byte lands, so a full disk, an
exceeded quota or a power cut destroys the previous good file and the
caller reports the failure as if nothing had been written.

The sequence below is the standard fix: write a temp file in the target's
own directory (an unpredictable name, so two concurrent writers cannot
share and clobber one temp), fsync the data, then ``os.replace`` onto the
target, which is an atomic rename on POSIX. A failure anywhere before the
replace leaves the previous file untouched, never truncated or empty. The
directory fsync afterwards is what makes the rename itself survive a power
loss.
"""

from __future__ import annotations

import os
import secrets
import stat
from pathlib import Path

# Flags an exclusive create needs: never follow a symlink into the name, and
# on Windows never translate line endings.
_CREATE_FLAGS = (
    os.O_WRONLY | os.O_CREAT | os.O_EXCL
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_BINARY", 0)
)

# A 16-hex-digit name collides with probability nil; the retries are for a
# hostile directory, not for chance.
_CREATE_ATTEMPTS = 100


def fsync_parent(path: Path) -> None:
    """Best-effort fsync of *path*'s directory, so the directory entry for a
    freshly created or renamed file survives a crash. A no-op on platforms
    without ``O_DIRECTORY`` (Windows)."""
    if not hasattr(os, "O_DIRECTORY"):
        return
    try:
        dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def _create_temp(path: Path, create_mode: int) -> tuple[int, str]:
    """Create an unpredictably named temp file beside *path*, returning its
    open descriptor and absolute name.

    *create_mode* is the mode an ordinary ``open`` would ask for, so the kernel
    applies the process umask to it. That is the only way to honour the umask:
    ``os.umask`` is process-wide state with no reader, so a library that probes
    it clears it for every other thread in the host process for the width of
    the probe, and a file that thread creates in that window lands with none of
    the host's masking. This package never calls ``os.umask``.
    """
    directory = os.path.abspath(str(path.parent))
    prefix = "." + path.name + "."
    for _ in range(_CREATE_ATTEMPTS):
        name = prefix + secrets.token_hex(8) + ".tmp"
        tmp_name = os.path.join(directory, name)
        try:
            return os.open(tmp_name, _CREATE_FLAGS, create_mode), tmp_name
        except FileExistsError:
            continue
    raise FileExistsError(f"no free temp name beside {path}")


def _replace(path: Path, data: bytes, *, mode: int | None) -> None:
    """Write *data* over *path* through a temp file and an atomic rename.

    *mode* is the permissions the finished file must have. ``None`` means the
    ones an ordinary write would have left, which only the kernel can supply.
    A stated mode is applied before the first byte, so the file never exists
    with looser perms than the caller asked for.
    """
    fd, tmp_name = _create_temp(path, 0o600 if mode is not None else 0o666)
    try:
        with os.fdopen(fd, "wb") as f:
            # fdopen owns fd now, so it is closed even if fchmod raises.
            if mode is not None:
                os.fchmod(f.fileno(), mode)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    # Best effort: os.replace has already committed the write, so a failure
    # here must not surface as a write failure.
    fsync_parent(path)


def atomic_write_bytes(
    path: Path, data: bytes, *, mode: int | None = None,
) -> None:
    """Write *data* to *path* so a failed write cannot destroy what is there.

    *mode* restates the permissions the caller needs (``0o600`` for a private
    key), so the file never exists with looser perms even if the umask
    changes. Left alone, the file stays owner-only.
    """
    _replace(path, data, mode=0o600 if mode is None else mode)


def _existing_mode(path: Path) -> int | None:
    """The mode *path* already carries, or ``None`` if it has none to keep.

    A replace hands the target the temp file's mode, so an export lands with
    whatever the temp was created with unless the previous file's own mode is
    carried over.
    """
    try:
        return stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return None


def atomic_write_text(
    path: Path, text: str, *, encoding: str = "utf-8", mode: int | None = None,
) -> None:
    """Text counterpart of :func:`atomic_write_bytes`.

    Left alone, the file lands with the permissions an ordinary write would
    have given it: the mode the target already had, or the umask for a new
    file. That is what a reviewer has to be able to open.
    """
    if mode is None:
        mode = _existing_mode(path)
    _replace(path, text.encode(encoding), mode=mode)
