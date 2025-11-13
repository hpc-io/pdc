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
  - Strengthen integration with HDF5, ADIOS, and other I/O frameworks.
  - Improve support for various network transports through Mercury (``ofi``, ``tcp``, ``verbs``, etc.).

**7.3.** Short-Term Goals (Next 6-12 Months)
--------------------------------------------

- Implement enhanced **data caching and eviction policies** to reduce I/O latency.
- Improve **burst buffer management** for hybrid memory/storage architectures.
- Extend **metadata indexing** and search capabilities.
- Add more comprehensive **unit and integration tests** in CI pipelines.
- Expand **PDC client APIs** in C and Python with better documentation and examples.

**7.4.** Medium-Term Goals (1-2 Years)
--------------------------------------

- Introduce **multi-tier data management** (memory, SSD, disk, object storage).
- Develop **asynchronous data movement and prefetching** mechanisms.
- Enhance **seurity and authentication** (e.g., Cray DRC, token-based access).
- Support **federated PDC deployments** across distributed sites.

**7.5.** Long-Term Vision (Beyond 2 Years)
------------------------------------------

- Enable **self-optimizing data placement** based on access patterns and system telemetry.
- Integrate with **workflow and data provenance frameworks** for end-to-end data lifecycle management.
- Expand **AI-driven data management features**, such as automated cache tuning and prediction-based prefetching.
- Achieve production-grade stability and adoption across DOE and large HPC facilities.

*Last updated: November 2025*
