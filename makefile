run:
	go run ./client/main.go

test:
	# rm -r /Users/hiroki/git/github.com/hirokihello/hhdb/src/tests/test_dir/
	go test -v ./...
