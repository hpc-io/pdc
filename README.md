[![linux](https://github.com/hpc-io/pdc/actions/workflows/linux.yml/badge.svg?branch=stable)](https://github.com/hpc-io/pdc/actions/workflows/linux.yml)
![GitHub release tag(latest by date)](https://img.shields.io/github/v/tag/hpc-io/pdc)
![Spack](https://img.shields.io/spack/v/pdc)

## Proactive Data Containers (PDC)
Proactive Data Containers (PDC) software provides an object-focused data management API, a runtime system with a set of scalable data object management services, and tools for managing data objects stored in the PDC system. The PDC API allows efficient and transparent data movement in complex memory and storage hierarchy. The PDC runtime system performs data movement asynchronously and provides scalable metadata operations to find and manipulate data objects. PDC revolutionizes how data is managed and accessed by using object-centric abstractions to represent data that moves in the high-performance computing (HPC) memory and storage subsystems. PDC manages extensive metadata to describe data objects to find desired data efficiently as well as to store information in the data objects.

Full documentation of PDC with installation instructions, code examples for using PDC API, and research publications are available at [pdc.readthedocs.io](https://pdc.readthedocs.io)

More information and publications on PDC is available at https://sdm.lbl.gov/pdc

If you use PDC in your research, please use the following citations:

```
@misc{byna:2017:pdc,
  title = {Proactive Data Containers (PDC) v0.1},
  author = {Byna, Suren and Dong, Bin and Tang, Houjun and Koziol, Quincey and Mu, Jingqing and Soumagne, Jerome and Vishwanath, Venkat and Warren, Richard and Tessier, François and USDOE},
  url = {https://www.osti.gov/servlets/purl/1772576},
  doi = {10.11578/dc.20210325.1},
  url = {https://www.osti.gov/biblio/1772576},
  year = {2017},
  month = {5},
}

@inproceedings{tang:2018:toward,
  title = {Toward scalable and asynchronous object-centric data management for HPC},
  author = {Tang, Houjun and Byna, Suren and Tessier, Fran{\c{c}}ois and Wang, Teng and Dong, Bin and Mu, Jingqing and Koziol, Quincey and Soumagne, Jerome and Vishwanath, Venkatram and Liu, Jialin and others},
  booktitle = {2018 18th IEEE/ACM International Symposium on Cluster, Cloud and Grid Computing (CCGRID)},
  pages = {113--122},
  year = {2018},
  organization = {IEEE}
}

@inproceedings{tang:2019:tuning,
  title = {Tuning object-centric data management systems for large scale scientific applications},
  author = {Tang, Houjun and Byna, Suren and Bailey, Stephen and Lukic, Zarija and Liu, Jialin and Koziol, Quincey and Dong, Bin},
  booktitle = {2019 IEEE 26th International Conference on High Performance Computing, Data, and Analytics (HiPC)},
  pages = {103--112},
  year = {2019},
  organization = {IEEE}
}
```