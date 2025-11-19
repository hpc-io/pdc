**7.** PDC Project Roadmap
===========================

**7.1.** Overview
-----------------

This roadmap outlines the current focus areas, near-term development goals, and long-term
vision for PDC.

**7.2.** Current Focus (Ongoing)
--------------------------------

- **Transformations**
  - Research is currently focused on enabling flexible and efficient in-flight data transformations within PDC. 
    The goal is to allow data to be transformed transparently during movement between storage and memory, reducing 
    application complexity and improving performance. Efforts include defining abstractions for expressing transformation pipelines, 
    integrating them with I/O operations, and exploring strategies to optimize execution without requiring 
    manual intervention from application developers.

- **User Experience**
  - Streamline configuration with clearer environment variables and default parameters.
  - Expand documentation and example workflows for common HPC use cases.

- **Interoperability**
  - Strengthen integration with HDF5 and other I/O frameworks.

**7.3.** Short-Term Goals (Next 6-12 Months)
--------------------------------------------

- Implement enhanced client-side data caching and eviction 
  policies to automatically exchange data from other MPI ranks and from multiple nodes.

**7.4.** Medium-Term Goals (1-2 Years)
--------------------------------------

- Integrate data movement between multiple HPC systems and between HPC and Cloud object storage systems.
- Compound data type support for regions.

**7.5.** Long-Term Vision (Beyond 2 Years)
------------------------------------------

- Integrate with workflow and data provenance frameworks for end-to-end data lifecycle management.
- Expand AI-driven data management features, such as automated cache tuning and prediction-based prefetching.
- Achieve production-grade stability and adoption across DOE and large HPC facilities.
- Integrate PDC in DOE applications and workflows.

*Last updated: November 2025*
