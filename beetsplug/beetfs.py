import os, stat, errno, datetime
# pull in some spaghetti to make this stuff work without fuse-py being installed
try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
import errno
from fuse import Fuse
from beets import config as Config
from beets.plugins import BeetsPlugin
from beets.ui import Subcommand

PATH_FORMAT = Config['paths']['default'].get()
PATH_FORMAT_SPLIT = PATH_FORMAT.split('/')

mount_command = Subcommand('mount', help='mount a beets filesystem')
def mount(lib, opts, args):
    global fs_tree
    fs_tree = build_fs_tree(lib)
    """
    for f in fs_tree.children:
        print(f.name)
        for g in f.children:
            print(g.name)
            for h in g.children:
                print(h.name)
    print("PATH_FORMAT = ", PATH_FORMAT)
    """
    usage = "where and when will this print?"
    server = BeetFuse(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')
    server.parse(errex=1)
    server.main()
mount_command.func = mount

def build_fs_tree(library):
    items = list(library.items())
    root = TreeNode()
    height = len(PATH_FORMAT_SPLIT)
    for item in items:
        cursor = root
        for depth in range(0, height):
            name = item.evaluate_template(PATH_FORMAT_SPLIT[depth])
            child = TreeNode(name, cursor)
            cursor = cursor.add_child(child)
    return root

class BeetFs(BeetsPlugin):
    def commands(self):
        return [mount_command]

class TreeNode():
    def __init__(self, name='root', parent=None):
        self.name = name
        self.parent = parent
        self.children = []

    def add_child(self, child):
        for _child in self.children:
            if _child.name == child.name:
                return _child
        else:
            self.children.append(child)
            return child

    def find(self, target): # linear search :(
        if self.name == target.name:
            return self
        for child in self.children:
            result = child.find(target)
            if result:
                return result
        
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

class BeetFuse(Fuse):
    def getattr(self, path):
        return -errno.ENOSYS

    def readdir(self, path, offset):
        path_split = path.split('/')[1:]
        height = len(path_split)
        root = fs_tree
        if path == '/':
            dirs = root
        else:
            for depth in range(0,height):
                root = root.find(path_split[depth])
            dirs = root
        for d in  ('.', '..', *(x.name for x in dirs.children)):
            yield fuse.Direntry(d)

    def open(self, path, flags):
        return -errno.ENOSYS

    def read(self, path, size, offset):
        return -errno.ENOSYS
