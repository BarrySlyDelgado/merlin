PYTHON?=python3
PYV=$(shell $(PYTHON) -c "import sys;t='{v[0]}_{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
PYVD=$(shell $(PYTHON) -c "import sys;t='{v[0]}.{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
VENV?=venv_merlin_$(SYS_TYPE)_py$(PYV)
CERT?=/etc/pki/tls/cert.pem
PIP?=$(VENV)/bin/pip
PYTH?=$(VENV)/bin/python
MRLN?=merlin/
TEST?=tests/
MAX_COMPLEXITY?=5
VENVMOD?=venv

PENV=merlin$(PYV)

.PHONY : all
.PHONY : install
.PHONY : virtualenv
.PHONY : install-pip-mysql
.PHONY : install-tasks
.PHONY : install-scipy
.PHONY : update
.PHONY : pull
.PHONY : clean-output
.PHONY : clean-py
.PHONY : clean
.PHONY : unit-tests
.PHONY : cli-tests
.PHONY : tests
.PHONY : fix-style
.PHONY : check-style
.PHONY : check-camel-case
.PHONY : checks
.PHONY : start-workers


all: install install-tasks install-pip-mysql install-sphinx


# install requirements
install: virtualenv
	$(VENV)/bin/easy_install cryptography
	$(PIP) install --cert $(CERT) -r requirements.txt


# this only works outside the venv
virtualenv:
	$(PYTHON) -m $(VENVMOD) $(VENV) --prompt $(PENV) --system-site-packages
	$(PIP) install --cert $(CERT) --upgrade pip


install-sphinx:
	$(PIP) install --upgrade sphinx


install-pip-mysql:
	$(PIP) install -r requirements/mysql.txt


install-tasks:
	$(PIP) install -e .


install-scipy:
	$(PIP) install --cert $(CERT) scipy --ignore-installed


# this only works outside the venv
update: pull install clean


pull:
	git pull


# remove python bytecode files
clean-py:
	-find $(MRLN) -name "*.py[cod]" -exec rm -f {} \;
	-find $(MRLN) -name "__pycache__" -type d -exec rm -rf {} \;


# remove all studies/ directories
clean-output:
	-find $(MRLN) -name "studies*" -type d -exec rm -rf {} \;
	-find workflows/ -name "studies*" -type d -exec rm -rf {} \;
	-find . -maxdepth 1 -name "studies*" -type d -exec rm -rf {} \;
	-find . -maxdepth 1 -name "merlin.log" -type f -exec rm -rf {} \;


# clean out unwanted files
clean: clean-py


unit-tests:
	-$(PYTH) -m pytest $(TEST)


# run CLI tests
cli-tests:
	-$(PYTH) $(TEST)integration/run_tests.py


# run unit and CLI tests
tests: unit-tests cli-tests


# automatically make python files pep 8-compliant
fix-style:
	isort -rc $(MRLN)
	isort -rc $(TEST)
	black --target-version py36 $(MRLN)
	black --target-version py36 $(TEST)


# run code style checks
check-style:
	-$(PYTH) -m flake8 --max-complexity $(MAX_COMPLEXITY) --exclude ascii_art.py $(MRLN)
	-black --check --target-version py36 $(MRLN)


# finds all strings in project that begin with a lowercase letter,
# contain only letters and numbers, and contain at least one lowercase
# letter and at least one uppercase letter.
check-camel-case: clean-py
	grep -rnw --exclude=lbann_pb2.py $(MRLN) -e "[a-z]\([A-Z0-9]*[a-z][a-z0-9]*[A-Z]\|[a-z0-9]*[A-Z][A-Z0-9]*[a-z]\)[A-Za-z0-9]*"


# run all checks
checks: check-style check-camel-case


# basic shortcut for starting celery workers
start-workers:
	celery worker -A merlin -l INFO
