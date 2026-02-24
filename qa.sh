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
mypy . --explicit-package-bases
echo "All checks passed successfully!"
