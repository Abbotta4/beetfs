# beetfs

beetfs mounts a FUSE that provides all your music with the desired tags and metadata applied to the files without touching the actual files underneath.

beetfs takes a config option for building the directory structure of your beets filesystem. The format is the same that is used in `paths`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
```

To mount the filsystem, simply give the mountpoint as an argument to `beet mount`, e.g. `beet mount ~/Music/beetfs`. A beets query can optionally be passed with `-q` to filter the database, e.g. `beet mount -q 'soundtrack:1 ~/Music/Soundtracks`

One or more mountpoints can be specified in the config file with beets queries and will be mounted with just `beet mount`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
    mounts:
        /home/foo/beets/bmnt:
        /home/foo/beets/soundtracks: soundtrack:1
        /home/foo/beets/music: soundtrack:0
```

## Install

To use beetfs, do any of:
* copy `beetfs.py` directly into your `beetsplug` directory
* install it from [PyPi](https://pypi.org/project/beets-beetfs/) with `pip`: `pip install beets-beetfs`
* install it with your package manager (a PKGBUILD for Arch users can be found [here](https://github.com/Abbotta4/beets-beetfs))

and configure it by adding the aforementioned config option to your `config.yaml`. beetfs depends on [pyfuse3](https://github.com/libfuse/pyfuse3), so be sure to install it with `pip` or your distro's package manager.