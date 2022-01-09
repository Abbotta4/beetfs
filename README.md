# beetfs

beetfs takes a single config option for building the directory structure of your beets filesystem. The format is the same that is used in `paths`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
```

To mount the filsystem, simply give the mountpoint as an argument to `beet mount`, e.g. `beet mount ~/Music/beetfs`.

## Install

To use beetfs, do any of:
* copy `beetfs.py` directly into your `beetsplug` directory
* install it from [PyPi](https://pypi.org/project/beets-beetfs/) with `pip`: `pip install beets-beetfs`
* install it with your package manager (a PKGBUILD for Arch users can be found [here](https://github.com/Abbotta4/beets-beetfs))

and configure it by adding the aforementioned config option to your `config.yaml`. beetfs depends on [pyfuse3](https://github.com/libfuse/pyfuse3), so be sure to install it with `pip` or your distro's package manager.