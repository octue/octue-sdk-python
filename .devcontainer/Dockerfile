FROM windpioneers/gdal-python:rational-swordtail-gdal-3.10.0-python-3.13-dev

# Tell zsh where you want to store history
#     We leave you to decide, but if you put this into a folder that's been mapped
#     into the container, then history will persist over container rebuilds :)
#
#     !!!IMPORTANT!!!
#     Make sure your .zsh_history file is NOT committed into your repository, as it can contain
#     sensitive information. So in this case, you should add
#         .devcontainer/.zsh_history
#     to your .gitignore file.
#
ENV HISTFILE="/workspaces/octue-sdk-python/.devcontainer/.zsh_history"

# Install poetry
USER vscode
ENV POETRY_HOME=/home/vscode/.poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN poetry config virtualenvs.create false
