# Hydrabadger

An experimental peer-to-peer client using the [Honey Badger Byzantine Fault
Tolerant consensus algorithm](https://github.com/poanetwork/hbbft).

## Usage

### Compile

1. `git clone -b android git@github.com:poanetwork/hydrabadger.git`
2. `cd hydrabadger`

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

8. `./compile`

### Current State

Network initialization node addition, transaction generation, consensus,
and batch outputs are all generally working. Batch outputs for each epoch are
printed to the log.

Overall the client is fragile and doesn't handle deviation from simple usage
very well yet.

### Unimplemented

* **Many edge cases and exceptions:** disconnects, reconnects, etc.
  * Connecting to a network which is in the process of key generation causes
    the entire network to fail. For now, wait until the network starts
    outputting batches before connecting additional peer nodes.
* **Error handling** is atrocious, most errors are simply printed to the log.
* **Usage as a library** is still a work in progress as the API settles.
* **Much, much more...**

### License

[![License: LGPL v3.0](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)

This project is licensed under the GNU Lesser General Public License v3.0. See the [LICENSE](LICENSE) file for details.
