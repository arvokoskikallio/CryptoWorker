# noqa: D100

from setuptools import setup

setup(
    name="quantaworker",
    version="0.1.0",
    description="Python worker client for Quanta",
    author="Vilppu Vuorinen",
    author_email="vilppu.vuorinen@jubic.fi",
    license="MIT",
    url="https://github.com/jubicoy/quanta-worker",
    python_requires=">3.5",
    packages=["quantaworker"],
    keywords=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    install_requires=["pandas", "python-dotenv", "requests"],
    scripts=[],
)
