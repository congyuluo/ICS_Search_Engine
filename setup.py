from setuptools import setup
from Cython.Build import cythonize

# File for compiling cython code

setup(
    ext_modules = cythonize("cython_defs.pyx")
)