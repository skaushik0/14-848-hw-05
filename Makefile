SRC_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
DAT_DIR := /src/data
OUT_DIR := $(SRC_DIR)/out
RUN_PRG := /src/search.py
DKR_IMG := mcr.microsoft.com/mmlspark/release

lint:
	pylint "$(SRC_DIR)/search.py"

search: lint
	docker run --rm -v \
	"$(SRC_DIR):/src" "$(DKR_IMG)" "$(RUN_PRG)" "$(ARG)" "$(DAT_DIR)"

clean:
	rm out/*.txt

test: clean
	echo "# test: Get the top 5 terms and their words frequencies." > \
	$(OUT_DIR)/test-top-5-terms.txt

	docker run --rm -v "$(SRC_DIR):/src" "$(DKR_IMG)" "$(RUN_PRG)" "5" \
	"$(DAT_DIR)" >> $(OUT_DIR)/test-top-5-terms.txt


	echo '# test: Search for the terms: king, war, peace and caesar.' > \
	$(OUT_DIR)/test-search.txt

	docker run --rm -v "$(SRC_DIR):/src" "$(DKR_IMG)" "$(RUN_PRG)" \
	"king,war,peace,caesar" "$(DAT_DIR)" >> $(OUT_DIR)/test-search.txt


	echo '# test: Check that stop words are filtered out from the index.' \
	>> $(OUT_DIR)/test-stop-words.txt

	docker run --rm -v  "$(SRC_DIR):/src" "$(DKR_IMG)" "$(RUN_PRG)" "the" \
	"$(DAT_DIR)" >> $(OUT_DIR)/test-stop-words.txt

.PHONY: lint search clean test
