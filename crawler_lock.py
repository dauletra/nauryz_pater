"""Shared file lock — prevents simultaneous cron + /run execution."""
import logging

LOCK_PATH = "/var/lock/otbasy_crawler.lock"

logger = logging.getLogger(__name__)

try:
    import fcntl as _fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False  # Windows — no locking needed


def acquire():
    """Try to acquire exclusive lock. Returns fd on success, None if already locked."""
    if not _HAS_FCNTL:
        return object()  # Windows stub
    try:
        fd = open(LOCK_PATH, "w")
        _fcntl.flock(fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        return None


def release(fd) -> None:
    if not _HAS_FCNTL or not hasattr(fd, "fileno"):
        return
    try:
        _fcntl.flock(fd, _fcntl.LOCK_UN)
        fd.close()
    except Exception:
        pass
