build:
	docker build -t book-crawler:latest .

run: build
	docker run --rm -i book-crawler:latest > graph.dot

show:
	./dot-to-svg.sh && open graph.svg

run-show: run show

