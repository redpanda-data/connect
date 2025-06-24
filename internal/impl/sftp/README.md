# SFTP components

## Localhost Docker setup

The https://github.com/drakkan/sftpgo project offers a fully-featured SFTP server packaged as a [Docker container](https://hub.docker.com/r/drakkan/sftpgo).

Run the `drakkan/sftpgo` container:

```shell
$ mkdir sftp && cd sftp
$ docker run --rm -it -p 8080:8080 -p 2022:2022 -v $(pwd):/srv/sftpgo -e SFTPGO_DATA_PROVIDER__CREATE_DEFAULT_ADMIN=true -e SFTPGO_DEFAULT_ADMIN_USERNAME=admin -e SFTPGO_DEFAULT_ADMIN_PASSWORD=password drakkan/sftpgo:edge-alpine-slim
```

Setup an account in the container:

```shell
$ BASE_URL="localhost:8080/api/v2"
$ TOKEN_URL="http://admin:password@${BASE_URL}/token"
$ RESPONSE=$(curl -s --show-error ${TOKEN_URL})
$ TOKEN=$(
  echo ${RESPONSE} \
  | jq ".access_token" \
  | sed 's/^"\(.*\)"$/\1/'
)
$ curl --request POST \
  --url ${BASE_URL}/users \
  --header "Authorization: Bearer ${TOKEN}" \
  --header "Content-Type: application/json; charset=utf-8" \
  --data '{"id": 1, "status": 1, "username": "admin", "password": "password", "permissions": {"/": ["*"]}}'
$ ssh-keyscan -t ssh-ed25519 -p 2022 127.0.0.1 | sed -n "s/^[^ #]* //p" > sftpgo.pub
```

You should now be able to access the SFTPGo web UI via http://localhost:8080 with user `admin` and password `password`.

The SFTP server should be accessible via `localhost:2022` with user `admin` and password `password`. You'll first have
to add its public key to your [`known_hosts` file](https://man7.org/linux/man-pages/man1/ssh.1.html#AUTHENTICATION) or,
alternatively, you can configure the `credentials.host_public_key_file` of the `sftp` input and / or output to point to
the `sftpgo.pub` generated above via `ssh-keyscan`.
