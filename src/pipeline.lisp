(defpackage :dew-target
  (:use :cl)
  (:export :main))

(in-package :dew-target)

(defparameter *verbose* t)

(defun read-input (file)
  (when *verbose*
    (format t "Processing input file ~A~%" file))
  (cl-csv:read-csv file :separator #\;))

(defun write-output (file data)
  (when *verbose*
    (format t "Writing output to file ~A~%" file))
  (with-open-file (f file
                     :if-does-not-exist :create
                     :direction :output
                     :if-exists :supersede)
    (cl-csv:write-csv data :stream f :separator #\;)))

(defun parse-float (s)
  "Parse float string in s"
  (+ 0.0d0 (with-input-from-string (in s)
             (read in))))

(defun parse-int (s)
  "Parse int string in s"
  (with-input-from-string (in s)
    (read in)))

(defun timestamp-range (year &key (step 1) (unit :day))
  "Generate a range of timestamps for the given year"
  (let* ((start-date (local-time:encode-timestamp 0 0 0 0 1 1 year))
         (end-date (local-time:encode-timestamp 0 0 0 0 31 12 year))
         (current start-date))
    (loop while (local-time:timestamp<= current end-date)
          :collect current
          :do (setq current (local-time:timestamp+ current step unit)))))

(defun calculate-daily-targets (target email date-range)
  (let* ((day-count (length date-range))
         (daily-target (/ target day-count)))
    (loop for date in date-range
          :collect (list
                    (local-time:format-timestring nil date :format '(:day "/" :month "/" :year))
                    email
                    daily-target))))

(defun process-record (record)
  (let* ((year (parse-int (first record)))
         (email (second record))
         (target (parse-float (third record)))
         (range (timestamp-range year)))
    (calculate-daily-targets target email range)))

(defun prepend-header (data)
  (cons '("data" "collaborator" "target") data))

(defun run-pipeline (inputfile outputfile)
  (arrows:->>
   (read-input (truename inputfile))
   (mapcan #'process-record)
   (prepend-header)
   (write-output outputfile)))

(defun display-help ()
  (opts:describe
   :prefix "Dewaele Target Pipeline. Usage:"
   :args "[keywords]") ;; to replace "ARG" in "--nb ARG"
  (uiop:quit))

(defun main ()
  (opts:define-opts
      (:name :help
             :description "print this help text"
             :short #\h
             :long "help")
      (:name :inputfile
             :description "the input file to process"
             :short #\i
             :long "input"
             :arg-parser #'identity
             :meta-var "FILE")
    (:name :outputfile
           :description "the file to write the output to"
           :short #\o
           :long "output"
           :arg-parser #'identity
           :meta-var "FILE"))

  (multiple-value-bind (options free-args)
      (opts:get-opts)

    (if (getf options :help)
        (display-help))
    
    (unless (and (getf options :inputfile) (getf options :outputfile))
      (display-help))
    
    (let ((inputfile (getf options :inputfile))
          (outputfile (getf options :outputfile)))
      (run-pipeline inputfile outputfile))))
