# PDC Documentation

This walks you through setting up a local environment
for building the PDC project documentation.

---

## Requirements

Ensure the following versions are installed on your system. Other versions
may also work but have not been tested.

- **Python 3.8.18**
- **pip 25.0.1**
- **Doxygen 1.13.2**

You can check the versions with:
> ```bash
> python3.8 --version
> pip --version
> doxygen --version
> ```

---

## Setup Instructions

### 1. Clone the repository (if not already)
```bash
git clone https://github.com/hpc-io/pdc.git
cd pdc/docs
```

### 2. Create Python environment install dependencies
```bash
python3.8 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Build the html
```bash
python3.8 -m sphinx -T -b html -d _build/doctrees -D language=en source html
```
