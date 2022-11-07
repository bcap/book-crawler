build:
	docker build -t book-crawler:latest .

run: build
	docker run --rm -i book-crawler:latest

run-graph: build
	docker run --rm -i book-crawler:latest | tee graph.dot && ./dot-to-svg.sh && open graph.svg

shell: build
	docker run --rm -it --entrypoint /bin/bash book-crawler:latest

