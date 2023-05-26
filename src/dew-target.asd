(asdf:defsystem #:dew-target
  :description "A target processing pipeline"
  :version "0.1.0"
  :author "Michael Anckaert <michael.anckaert@sinax.be"
  :license "MIT"
  :serial t
  :depends-on (#:cl-csv
               #:str
               #:arrows
               #:local-time
               #:unix-opts)
  :build-operation "program-op"
  :build-pathname "dew-target"
  :entry-point "dew-target:main"
  :components ((:file "pipeline")))

#+sb-core-compression
(defmethod asdf:perform ((o asdf:image-op) (c asdf:system))
  (uiop:dump-image (asdf:output-file o c) :executable t :compression t))
