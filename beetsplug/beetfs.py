import os, stat, errno, datetime
# pull in some spaghetti to make this stuff work without fuse-py being installed
try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
from fuse import Fuse
from beets import config as Config
from beets.plugins import BeetsPlugin
from beets.ui import Subcommand

PATH_FORMAT = Config['paths']['default'].get()
PATH_FORMAT_SPLIT = PATH_FORMAT.split('/')

mount_command = Subcommand('mount', help='mount a beets filesystem')
def mount(lib, opts, args):
    print("PATH_FORMAT = ", PATH_FORMAT)
    """
    usage = "where and when will this print?"
    server = beet_fuse(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')
    server.parse(errex=1)
    server.main()
    """
mount_command.func = mount

class beetfs(BeetsPlugin):
    def commands(self):
        return [mount_command]

if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

uid = os.getuid()
gid = os.getgid()

class Stat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = uid
        self.st_gid = gid
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class beet_fuse(Fuse):
    def getattr(self, path):
        pass

    def readdir(self, path, offset):
        pass

    def open(self, path, flags):
        pass

    def read(self, path, size, offset):
        pass
