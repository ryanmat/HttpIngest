## Python

- I prefer to use uv for everything (uv add, uv run, etc)
- Do not use old fashioned methods for package management like poetry, pip or easy_install.
- Make sure that there is a pyproject.toml file in the root directory.
- If there isn't a pyproject.toml file, create one using uv by running uv init.



### Prompt: Project Clean-Up

```
Review and think on the entire project.
- I want to lean out the project and remove all bloat:
    - Remove all machine learning.  This project is going to feed data for my seperate prediction model as well as being a HTTP demo environment for Data Publisher.
    - Look through the rest of the project and give me recommendations for things to remove.
    - After the project is leaned out. Concentrate on what features we have built and:
        - Confirm everything is connected.
        - All tests pass.
        - Test data coming in from LogicMonitor Data Publisher.
- Check and modify all files with new CLAUDE.MD instructions.
- Review and remove all emojis from the project.
- Review the gitignore and add all claude files and folders to the gitignore to not confuse other environments.  This includes files from the docs: docker-uv.md, python.md, source-control.md, using-uv.md.
```