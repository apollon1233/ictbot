PY=python

.PHONY: smoke
smoke:
	$(PY) scripts/smoke_check.py

.PHONY: smoke-strict
smoke-strict:
	$(PY) scripts/smoke_check.py --strict

