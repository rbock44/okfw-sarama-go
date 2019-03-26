GO=GO111MODULE=on go

all: update generate bench test

update:
	cd sarama && $(GO) mod vendor

generate:
	cd sarama && $(GO) generate -mod=vendor

bench: 
	cd sarama && $(GO) test -mod=vendor -bench=.

test:
	docker-compose down >/dev/null 2>&1
	docker-compose up >/dev/null 2>&1 &
	sleep 40 && cd sarama && $(GO) test 
	docker-compose down >/dev/null 2>&1


