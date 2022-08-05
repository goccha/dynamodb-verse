# dynamodb-verse


## Installation
```shell
go install github.com/goccha/dynamodb-verse@latest ./...
```

## migration

```shell
dynamodb-migrate --path=configs/dynamodb --endpoint=http://localhost:8081
```

### parameters

| key      | default          | description                                      | example               |
|----------|------------------|--------------------------------------------------|-----------------------|
| local    | true             | Set "http://localhost:8000" if endpoint is empty | false                 |
| region   | ap-northeast-1   | aws region                                       | ap-northeast-1        |
| endpoint |                  | dynamodb endpoint                                | http://localhost:8000 |
| profile  |                  | aws profile name                                 | default               |
| path     | configs/dynamodb | config directory path                            | deployments/resources |
| debug    |                  | aws sdk debug log                                | true                  |
| version  |                  | show version                                     |                       |
| h        |                  | help message                                     |                       |

### environment values
Arguments take precedence over environment variables.

| key                   | default | description           |
|-----------------------|---------|-----------------------|
| AWS_DEBUG_LOG         | false   | aws sdk debug log     |
| AWS_REGION            |         | aws region            |
| AWS_PROFILE           |         | aws profile name      |
| AWS_DYNAMODB_ENDPOINT |         | dynamodb endpoint     |
| DYNAMODB_CONFIG_PATH  |         | config directory path |

