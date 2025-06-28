.. _pdcpy:

**2.** PDCpy (Python Bindings)
==============================

With the rise of Python in the HPC community, PDC now provides a Python
binding called PDCpy, which allows users to interact with PDC using Python.
The repository for PDCpy can be found at `PDCpy GitHub Repository <https://github.com/hpc-io/PDCpy>`_.
The documentation for PDCpy's API is available at `PDCpy Documentation <https://hpc-io.github.io/PDCpy/>`_.

**2.1** Building PDCpy
----------------------

PDCpy is compatible with openmpi and mpich. If neither compiler is installed, 
it will attempt to compile without mpi support, which will fail if you compile 
pdc with mpi support.

**2.2** Running PDCpy
---------------------