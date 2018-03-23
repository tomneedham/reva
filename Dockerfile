FROM golang:latest

#RUN curl https://glide.sh/get | sh

RUN git clone https://github.com/cernbox/reva.git /go/src/reva -p owncloud
# ADD . /go/src/reva

# RUN cd /go/src/reva/reva-server && glide install && go build && go install
RUN cd /go/src/reva/reva-server && go build && go install
# RUN cd /go/src/reva/reva-cli && glide install && go build && go install
# RUN cd /go/src/reva/oc-proxy && glide install && go build && go install

ADD reva.yaml /etc/reva/reva.yaml
# ADD oc-proxy.yaml /etc/oc-proxy/oc-proxy.yaml

CMD reva-server
