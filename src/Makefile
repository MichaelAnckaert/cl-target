LISP ?= sbcl
PWD := $(shell pwd)

all: build

build: dew-target.asd pipeline.lisp
	$(LISP) --eval '(setf asdf:*central-registry* (list* *default-pathname-defaults*  #p"$(PWD)/" asdf:*central-registry*))' \
		--eval '(ql:quickload :dew-target)' \
		--eval '(asdf:make :dew-target)' \
		--eval '(quit)'
