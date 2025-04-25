# Makefile variables set automatically
plugin_id=`cat plugin.json | python -c "import sys, json; print(str(json.load(sys.stdin)['id']).replace('/',''))"`
plugin_version=`cat plugin.json | python -c "import sys, json; print(str(json.load(sys.stdin)['version']).replace('/',''))"`
archive_file_name="dss-plugin-${plugin_id}-${plugin_version}.zip"
remote_url=`git config --get remote.origin.url`
last_commit_id=`git rev-parse HEAD`

.DEFAULT_GOAL := plugin

plugin: dist-clean
	@echo "[START] Archiving plugin to dist/ folder..."
	@cat plugin.json | json_pp > /dev/null
	@mkdir dist
	@echo "{\"remote_url\":\"${remote_url}\",\"last_commit_id\":\"${last_commit_id}\"}" > release_info.json
	@git archive -v -9 --format zip -o dist/${archive_file_name} HEAD
	@if [[ -d tests ]]; then \
		zip --delete dist/${archive_file_name} "tests/*"; \
	fi
	@zip -u dist/${archive_file_name} release_info.json
	@rm release_info.json
	@echo "[SUCCESS] Archiving plugin to dist/ folder: Done!"

dev: dist-clean
	@echo "[START] Archiving plugin to dist/ folder... (dev mode)"
	@cat plugin.json | json_pp > /dev/null
	@mkdir dist
	@zip -v -9 dist/${archive_file_name} -r . --exclude "tests/*" "env/*" ".git/*" ".pytest_cache/*" ".idea/*" "dist/*"
	@echo "[SUCCESS] Archiving plugin to dist/ folder: Done!"

unit-tests:
	@echo "Running unit tests..."
	@( \
		PYTHON_VERSION=`python3 -c "import sys; print('PYTHON{}{}'.format(sys.version_info.major, sys.version_info.minor))"`; \
		PYTHON_VERSION_IS_CORRECT=`cat code-env/python/desc.json | python3 -c "import sys, json; print('$$PYTHON_VERSION' in json.load(sys.stdin)['acceptedPythonInterpreters']);"`; \
		if [ $$PYTHON_VERSION_IS_CORRECT == "False" ]; then echo "Python version $$PYTHON_VERSION is not in acceptedPythonInterpreters"; exit 1; else echo "Python version $$PYTHON_VERSION is in acceptedPythonInterpreters"; fi; \
	)
	@( \
		rm -rf ./env/; \
		python3 -m venv env/; \
		source env/bin/activate; \
		pip install --upgrade pip;\
		pip install --no-cache-dir -r tests/python/unit/requirements.txt; \
		pip install --no-cache-dir -r code-env/python/spec/requirements.txt; \
		export PYTHONPATH="$(PYTHONPATH):$(PWD)/python-lib"; \
		python3 -m pytest tests/python/unit --alluredir=tests/allure_report || ret=$$?; exit $$ret \
	)

integration-tests:
	@echo "Running integration tests..."
	@( \
		rm -rf ./env/; \
		python3 -m venv env/; \
		source env/bin/activate; \
		pip3 install --upgrade pip;\
		pip install --no-cache-dir -r tests/python/integration/requirements.txt; \
		python3 -m pytest tests/python/integration --alluredir=tests/allure_report || ret=$$?; exit $$ret \
	)

tests: unit-tests integration-tests

dist-clean:
	rm -rf dist