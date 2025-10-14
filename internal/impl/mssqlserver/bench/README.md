# Benchmarking Microsoft SQL Server CDC Component


```bash
$ docker-compose up -d
```

```bash
$ go run loader/main.go
```

Useful commands:

```bash
drop table rpcn.CdcCheckpointCache;


sp_msforeachtable 'EXEC sp_spaceused [?]'
```
