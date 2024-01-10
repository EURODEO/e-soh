from glob import glob
from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension

ext_modules = [
    Pybind11Extension(
        "esoh.ingest.bufr.bufresohmsg_py",
        sorted(glob("src/esoh/ingest/bufr/*.cpp")),  # Sort source files for reproducibility
        extra_compile_args=["-std=c++17"]
    ),
]


setup(ext_modules=ext_modules)
