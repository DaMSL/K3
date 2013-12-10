;;===============================================================
;; simple mode for K3
;;===============================================================

;; define several class of keywords
(setq k3-keywords '("declare" "trigger" "varD" "trgD" "annD" "then" "else" "if" "return" "annotation" "source" "sink" "feed" "value" "me" "bind" "as" "let" "in" "case" "of" "Some"))
(setq k3-types '("float" "integer" "key" "list" "rotation" "string" "vector" "int" "bool" "Collection" "lifted" "address"))
(setq k3-constants '("immut" "mut" "Null" "false" "true" "None"))
(setq k3-events '("attach" "provides"))
(setq k3-functions '("map" "maximum" "peek" "insert" "delete" "update" "combine" "split" "iterate" "filter" "fold" "groupBy" "ext" "&&" "||" "|>"))

;; create the regex string for each class of keywords
(setq k3-keywords-regexp (regexp-opt k3-keywords 'words))
(setq k3-type-regexp (regexp-opt k3-types 'words))
(setq k3-constant-regexp (regexp-opt k3-constants 'words))
(setq k3-event-regexp (regexp-opt k3-events 'words))
(setq k3-functions-regexp (regexp-opt k3-functions 'words))

;; clear memory
(setq k3-keywords nil)
(setq k3-types nil)
(setq k3-constants nil)
(setq k3-events nil)
(setq k3-functions nil)

;; create the list for font-lock.
;; each class of keyword is given a particular face
(setq k3-font-lock-keywords
  `(
    (,k3-type-regexp . font-lock-type-face)
    (,k3-constant-regexp . font-lock-constant-face)
    (,k3-event-regexp . font-lock-builtin-face)
    (,k3-functions-regexp . font-lock-function-name-face)
    (,k3-keywords-regexp . font-lock-keyword-face)
))

;; define command to comment/uncomment text

(defun k3-comment-dwim (arg)
  "Comment or uncomment current line or region in a smart way.
For detail, see `comment-dwim'."
  (interactive "*P")
  (require 'newcomment)
  (let (
        (comment-start "/*") (comment-end "*/")
        )
    (comment-dwim arg)))

;; syntax table
(defvar k3-syntax-table nil "Syntax table for `k3-mode'.")
(setq k3-syntax-table
      (let ((synTable (make-syntax-table)))

        (modify-syntax-entry ?\/ ". 14" synTable)
        (modify-syntax-entry ?* ". 23" synTable)

        synTable))


;; define the mode

(define-derived-mode k3-mode fundamental-mode
  "k3 mode"
  "Major mode for editing k3"
  :syntax-table k3-syntax-table

  ;; code for syntax highlighting
  (setq font-lock-defaults '((k3-font-lock-keywords)))
  (setq mode-name "k3")
  ;; clear memory
  (setq k3-keywords-regexp nil)
  (setq k3-types-regexp nil)
  (setq k3-constants-regexp nil)
  (setq k3-events-regexp nil)
  (setq k3-functions-regexp nil)

  (define-key k3-mode-map [remap comment-dwim] 'k3-comment-dwim) 
)

;;===============================================================
;; end of k3 mode
;;===============================================================
