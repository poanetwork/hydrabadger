#!/usr/bin/env python

from ctypes import *
cdll.LoadLibrary("./libhydrabadger.so")
libc = CDLL("./target/debug/libhydrabadger.so")
libc.rust_main2()
