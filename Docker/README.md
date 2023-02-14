# Running the image
You will need to replace the values in example.env with your CKAN API key and the url of the remote CKAN server you want to issue commands against before running.

After changing the .env variables and renaming example.env to .env:

1. cd into ./Docker
2. run `docker build . -t twdhcli`, then 
3. run `docker run --env-file ./.env twdhcli:latest`
4. run `docker exec {name of your running container} -it /bin/bash`
5. run your command using `$VENV/bin/python twdhcli.py {YOUR CMD}`

Things to note:
1. In this image/container, twdhcli is installed in a virtual environment.
2. The virtual environment path is stored in the env variable VENV
3. Given the two above, to run a script from the twdhcli, you will need to run python like `$VENV/bin/python`
4. You may also activate the venv with `. $VENV/bin/activate` to issue cmds simply using 'python' instead