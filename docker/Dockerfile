FROM golang:alpine
RUN mkdir /app
ADD . /app/
WORKDIR /app
RUN go build -o main .
RUN adduser -S -D -H -h /app appuser
USER appuser
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
CMD ["./main"]