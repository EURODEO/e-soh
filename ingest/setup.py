from glob import glob

from pybind11.setup_helpers import Pybind11Extension
from setuptools import setup

ext_modules = [
    Pybind11Extension(
        "ingest.bufr.bufresohmsg_py",
        sorted(glob("src/ingest/bufr/*.cpp")),  # Sort source files for reproducibility
        extra_compile_args=["-std=c++17"],
    ),
]


setup(ext_modules=ext_modules)
