# Copyright 2013-2023 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

from spack.package import *


class Pdc(CMakePackage):
    """Proactive Data Containers (PDC) software provides an object-centric
    API and a runtime system with a set of data object management services.
    These services allow placing data in the memory and storage hierarchy,
    performing data movement asynchronously, and providing scalable
    metadata operations to find data objects."""

    homepage = "https://pdc.readthedocs.io/en/latest/"
    url = "https://github.com/hpc-io/pdc/archive/refs/tags/0.3.tar.gz"
    git = "https://github.com/hpc-io/pdc.git"

    maintainers("houjun", "sbyna")

    version("0.3", sha256="14a3abd5e1e604f9527105709fca545bcdebe51abd2b89884db74d48a38b5443")
    version("0.2", sha256="2829e74da227913a1a8e3e4f64e8f422ab9c0a049f8d73ff7b6ca12463959f8b")
    version("0.1", sha256="01b4207ecf71594a7f339c315f2869b3fa8fbd34b085963dc4c1bdc5b66bb93e")

    version("stable", branch="stable")
    version("develop", branch="develop")

    conflicts("%clang")
    depends_on("libfabric")
    depends_on("mercury")
    depends_on("mpi")

    root_cmakelists_dir = "src"

    def cmake_args(self):
        args = [
            self.define("MPI_C_COMPILER", self.spec["mpi"].mpicc),
            self.define("BUILD_MPI_TESTING", "ON"),
            self.define("BUILD_SHARED_LIBS", "ON"),
            self.define("BUILD_TESTING", "ON"),
            self.define("PDC_ENABLE_MPI", "ON"),
            self.define("CMAKE_C_COMPILER", self.spec["mpi"].mpicc),
        ]

        if self.spec.satisfies("platform=cray"):
            args.append("-DRANKSTR_LINK_STATIC=ON")
        return args
