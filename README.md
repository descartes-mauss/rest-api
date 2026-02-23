# rest-api

Main REST API for Agentic Experience Project

## Runtime

- **Python**: 3.12.0 (see [.tool-versions](.tool-versions))

## Major dependencies

- **FastAPI**: high-performance ASGI framework for APIs
- **SQLModel**: ORM / models based on SQLAlchemy and Pydantic
- **SQLAlchemy**: core SQL toolkit used by `sqlmodel`
- **Uvicorn**: ASGI server for development and production
- **Pydantic**: data validation and settings management
- **httpx**: HTTP client used for testing or external requests
- **python-dotenv**: load environment variables from `.env`
- **python-jose**: JWT utilities
- **psycopg2-binary**: PostgreSQL driver
- See the full list in [requirements.txt](requirements.txt)

## Setup — manage tool versions with `asdf`

Install `asdf` (macOS/Homebrew alternative shown) and the Python plugin:

```bash
# Install asdf via git (recommended)
git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.12.0

# Add to shell (example for zsh):
echo '. $HOME/.asdf/asdf.sh' >> ~/.zshrc
echo '. $HOME/.asdf/completions/asdf.bash' >> ~/.zshrc
source ~/.zshrc

# Install Python plugin and desired version from .tool-versions
asdf plugin-add python || true
asdf install python 3.12.0
asdf global python 3.12.0
```

If you prefer Homebrew (macOS):

```bash
brew install asdf
asdf plugin-add python || true
asdf install python 3.12.0
asdf global python 3.12.0
```

## Create and use a virtual environment

```bash
# create venv in project
python -m venv .venv

# activate venv (macOS / Linux - zsh / bash)
source .venv/bin/activate

# install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

## Run (development)

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Notes

- Use the files [.tool-versions](.tool-versions) and [requirements.txt](requirements.txt) for reproducible runtime and dependency lists.
- On macOS, ensure Command Line Tools are installed for some packages (e.g., `psycopg2-binary` may require build tools in other setups).
