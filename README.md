# Frontier

This is the repository for storage logic in Frontier. Other related components are present at -

- [Frontier-enabled lambda](https://github.com/rahulgovind/corral) This repository includes a modified Corral repository
to run map-reduce jobs using serverless computing (Lambdas)
- [Fast selection](https://github.com/rahulgovind/select-simd) This repository includes a patched version of Minio's 
select-simd repository that lets us run select operations quickly on CSV data

## Introduction

The frontier package helps accelerate workloads that iteratively interact with S3. It lets you download files 
at a higher throughput, write back to S3 with a low latency, and lets you additionally filter data on-the-fly 
in order to increase the effective throughput at your client / compute node.

## Requirements
In order to setup Frontier, you will need the following
- Valid AWS Credentials that can access S3 and a valid bucket on S3 in the us-east-2 region to connect Frontier to
- Go installed locally or on the machine to run Frontier on 
- Redis installed locally or on one of the machines


## Local setup

Please store your AWS credentials in `~/.aws/credentials`. The initial setup for Frontier can be done by r
unning the following commands on an Ubuntu machine

```$xslt
sudo apt-get update
sudo apt-get -y upgrade
wget https://dl.google.com/go/go1.13.3.linux-amd64.tar.gz
sudo tar -xvf go1.13.3.linux-amd64.tar.gz
sudo mv go /usr/local
echo "export GOROOT=/usr/local/go" >> ~/.bash_profile
echo "export GOPATH=$HOME/go" >> ~/.bash_profile
echo 'export PATH=$GOPATH/bin:$GOROOT/bin:$PATH' >> ~/.bash_profile
source ~/.bash_profile
go get -u github.com/rahulgovind/fastfs
sudo apt install redis
```

## Running Frontier locally

Frontier can easily be run in a single node setting locally by running. It assumes a default local redis configuration 
Frontier can be contacted below by sending HTTP Requests to `http://localhost:8100` (NOTE: It is the given port + 100)
```$xslt
cd $GOPATH/github.com/rahulgovind/fastfs
go build -o main && ./main --bucket <bucket name> --port 8000
```

You can also add more nodes locally after this if you wish. The second node is started here on port 8001.

```$xslt
cd $GOPATH/github.com/rahulgovind/fastfs
go build -o main && ./main --bucket speedfs --port 8001 --primary-addr 127.0.0.1
```

## Testing Frontier locally

Assuming that everything above worked, we can now go through a few commands to work with Frontier

### Downloading a file
```$xslt
curl http://localhost:8100/data/<file-path-in-S3>
```

## Uploading a file
```$xslt
curl -X PUT http://localhost:8100/put/<file-path-in-S3> -T <path-to-local-file>
```

## List files in S3 directory

(The slash at the end is important)
```$xslt
curl http://localhsot:8100/ls/<s3-directory>/
```

## Querying files on S3

Frontier currently only exposes an API for equailty based filtering which can be done as follows - 

```$xslt
curl http://localhost:8100/query/<file-path-in-s3>?col=<column number>&cond=<condition>
```

This retries entries in column <col> which are equal to <condition>

## Client benchmarks

The `client-benchmarks` directory contains a number of scripts to interact with frontier using the Frontier 
Golang client library. A few are listed below

- client-benchmarks/upload2: Upload file using frontier client

- client-benchmarks/throughput2: Download file using frontier client

Building the corresponding packages using `go build` and adding the `-h` flag will list additional instructions.

## Map-Reduce using Frontier

The modified version of Corral to run map-reduce jobs using Frontier  is present at  
[Frontier-enabled lambda](https://github.com/rahulgovind/corral). There is some unfortunate hard coding here which 
has not yet been resolved. Namely `github.com/rahulgovind/corral/internal/pkg/corfs/fastfs.go` contains a hard coded link to the Ec2 machine we were
working with. AWS EC2 instances are recommended for corral due to proximity of the lambda instances and 
EC2 instances. Corral using Frontier has not been tested using local machines.

Next, set the environment variables `TEST_HOST` to point to your AWS instance and `AWS_TEST_BUCKET` to point to your
bucket.

Apart from this, Corral can immediately be used with Frontier. We have worked with the scripts in 
`corral/examples/word_count`. You can easily build the scripts using `make wc_lambda_xxx` commands in the Makefile.