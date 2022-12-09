build:
	docker build -t book-crawler:latest .

run: build
	docker run --network host --rm -i book-crawler:latest \
	"https://www.goodreads.com/book/show/3869.A_Brief_History_of_Time" --neo4j $(args)

run-graph: build
	docker run --network host --rm -i book-crawler:latest \
	"https://www.goodreads.com/book/show/3869.A_Brief_History_of_Time" --dot > graph.dot && \
	./dot-to-svg.sh graph.dot graph.svg && \
	open graph.svg

shell: build
	docker run --network host --rm -it --entrypoint /bin/bash book-crawler:latest

shellb: 
	docker build --target pre-build -t book-crawler:pre-build . && \
	docker run --network host --rm -it --entrypoint /bin/bash book-crawler:pre-build
