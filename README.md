# tokio_android

Try use tokio example on android

## Usage

### Compile to android

0. download
* Android SDK Tools
* NDK
* CMake
* LLDB

1. `git clone git@github.com:KORuL/tokio_android.git`
2. `cd tokio_android`

3. set needs environments 

`export ANDROID_HOME=/Users/$USER/Library/Android/sdk`

`export NDK_HOME=$ANDROID_HOME/ndk-bundle` 

and etc

4. make standalone NDK 

`${NDK_HOME}/build/tools/make_standalone_toolchain.py --api 26 --arch arm64 --install-dir NDK/arm64`
`${NDK_HOME}/build/tools/make_standalone_toolchain.py --api 26 --arch arm --install-dir NDK/arm`
`${NDK_HOME}/build/tools/make_standalone_toolchain.py --api 26 --arch x86 --install-dir NDK/x86`

5. set environment to NDK compilers and linkers

`export PATH=$PATH:<project path>/NDK/arm64/bin/`
`export PATH=$PATH:<project path>/NDK/arm/bin/`
`export PATH=$PATH:<project path>/NDK/x86/bin/`

6. make  cargo-config.toml 

`[target.aarch64-linux-android]`
`ar = "<project path>/NDK/arm64/bin/aarch64-linux-android-ar"`
`linker = "<project path>/NDK/arm64/bin/aarch64-linux-android-clang"`

`[target.armv7-linux-androideabi]`
`ar = "<project path>/NDK/arm/bin/arm-linux-androideabi-ar"`
`linker = "<project path>/NDK/arm/bin/arm-linux-androideabi-clang"`

`[target.i686-linux-android]`
`ar = "<project path>/NDK/x86/bin/i686-linux-android-ar"`
`linker = "<project path>/NDK/x86/bin/i686-linux-android-clang"'`

7. need copy this config file to our .cargo directory like this:

`cp cargo-config.toml ~/.cargo/config`

7.1. `rustup target add aarch64-linux-android armv7-linux-androideabi i686-linux-android`

8. `./compile`

It may also be necessary for the reed-solomon-erasure package to change the branch to dev

### Compile to linux

1. cargo build

### Current State

Compile to android, work fine on linux, but on android crash

### Test run Linux .so

1. `cargo build`
2. `./runTestPy`


### License

[![License: LGPL v3.0](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)

This project is licensed under the GNU Lesser General Public License v3.0. See the [LICENSE](LICENSE) file for details.
