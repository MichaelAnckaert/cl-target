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

(defun calculate-daily-targets (target identifier date-range &optional unit)
  (let* ((day-count (length date-range))
         (daily-target (/ target day-count)))
    (if (eq unit nil)
        (loop for date in date-range
              :collect (list
                        (local-time:format-timestring nil date :format '(:day "/" :month "/" :year))
                        identifier
                        daily-target))
        (loop for date in date-range
              :collect (list
                        (local-time:format-timestring nil date :format '(:day "/" :month "/" :year))
                        identifier
                        unit
                        daily-target)))))

(defun process-collaborator-record (record)
  (let* ((year (parse-int (first record)))
         (email (second record))
         (target (parse-float (third record)))
         (range (timestamp-range year)))
    (calculate-daily-targets target email range)))

(defun process-office-record (record)
  (let* ((year (parse-int (first record)))
         (office (second record))
         (unit (third record))
         (target (parse-float (fourth record)))
         (range (timestamp-range year)))
    (calculate-daily-targets target office range unit)))

(defun prepend-collaborator-header (data)
  (cons '("date" "collaborator" "target") data))

(defun prepend-office-header (data)
  (cons '("date" "office" "unit" "target") data))

(defun run-collaborator-pipeline (inputfile outputfile)
  (arrows:->>
   (read-input (truename inputfile))
   (mapcan #'process-collaborator-record)
   (prepend-collaborator-header)
   (write-output outputfile)))

(defun run-office-pipeline (inputfile outputfile)
  (arrows:->>
   (read-input (truename inputfile))
   (mapcan #'process-office-record)
   (prepend-office-header)
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
    (:name :type
     :description "the type of target to process"
     :short #\t
     :long "type"
     :arg-parser #'identity
     :default "collaborator"
     :meta-var "TYPE")
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
      (if (string= (getf options :type) "office")
          (run-office-pipeline inputfile outputfile)          
          (run-collaborator-pipeline inputfile outputfile)))))
