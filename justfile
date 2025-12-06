set shell := ["bash", "-cu"]

lexdir := "../lexicons"

lexgen:
	go run ./cmd/lexgen/ --build-file cmd/lexgen/vylet.json {{lexdir}}

cborgen:
	go run ./gen
