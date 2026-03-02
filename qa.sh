# /usr/bin/zsh

echo "Running quality assurance checks..."
echo "=============================="
echo "Running Black..."
black .
echo "Running iSort..."
isort .
echo "Running Ruff..."
ruff check . --unsafe-fixes
echo "Running Mypy..."
python -m mypy --show-error-context --explicit-package-bases --config-file "pyproject.toml" $(git ls-files "*.py" -- ':!tests/**')
echo "All checks passed successfully!"
