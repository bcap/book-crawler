build:
	docker build -t book-crawler:latest .

run: build
	docker run --rm -i book-crawler:latest "https://www.goodreads.com/book/show/3869.A_Brief_History_of_Time"

run-large: build
	docker run --rm -i book-crawler:latest "https://www.goodreads.com/book/show/3869.A_Brief_History_of_Time" -d 10 -r 3 > graph.dot

run-graph: build
	docker run --rm -i book-crawler:latest "https://www.goodreads.com/book/show/3869.A_Brief_History_of_Time" > graph.dot && ./dot-to-svg.sh && open graph.svg

shell: build
	docker run --rm -it --entrypoint /bin/bash book-crawler:latest

