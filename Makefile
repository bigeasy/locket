all: css/design.css

watch: all
	@inotifywait -q -m -e close_write css/*.css | while read line; do make --no-print-directory all; done;

css/%.css: css/%.less
	node_modules/.bin/lessc $< > $@
