# Copyright 2013-2020 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

# ----------------------------------------------------------------------------
# If you submit this package back to Spack as a pull request,
# please first remove this boilerplate and all FIXME comments.
#
# This is a template package file for Spack.  We've put "FIXME"
# next to all the things you'll want to change. Once you've handled
# them, you can save this file and test your package like this:
#
#     spack install pdc
#
# You can edit this file again by typing:
#
#     spack edit pdc
#
# See the Spack documentation for more information on packaging.
# ----------------------------------------------------------------------------

from spack import *


class Pdc(CMakePackage):
    """Proactive Data Containers (PDC) software provides an object-centric API and a runtime system with a set of 
       data object management services. These services allow placing data in the memory and storage hierarchy, 
       performing data movement asynchronously, and providing scalable metadata operations to find data objects."""

    homepage = "https://pdc.readthedocs.io/en/latest/"
    url      = "https://github.com/hpc-io/pdc/archive/refs/tags/0.1.tar.gz"

    maintainers = ['houjun', 'sbyna']

    version('0.1', sha256='24787806a30cd1cda1fed17220a62e768bdba5de56877f2ea7126279ff2a4f69')

    conflicts('%clang')
    depends_on('libfabric@1.11.2')
    depends_on('mercury')
    depends_on('cmake')
    depends_on('mpi')

    root_cmakelists_dir = 'src'

    def cmake_args(self):
        args = []
        args.append("-DMPI_C_COMPILER=%s" % self.spec['mpi'].mpicc)
        args.append("-DBUILD_MPI_TESTING=ON")
        args.append("-DBUILD_SHARED_LIBS=ON")
        args.append("-DBUILD_TESTING=ON")
        args.append("-DPDC_ENABLE_MPI=ON")
        args.append("-DCMAKE_C_COMPILER=%s" % self.spec['mpi'].mpicc)

        if self.spec.satisfies('platform=cray'):
            args.append("-DRANKSTR_LINK_STATIC=ON")
        return args
